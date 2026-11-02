package com.ispengya.polyconfig.core.distro;

import com.ispengya.polyconfig.common.codec.CommandRecord;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.MutationResult;
import com.ispengya.polyconfig.common.protocol.DistroSyncRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * owner 侧异步复制器（docs/ap-design.md §3）。
 *
 * <p>本地写（proposeLocal）刷盘成功后入队，定时攒批经 {@code DISTRO_SYNC}
 * 推送给其余副本。应答 owner = 已持久化，复制异步——副本短暂落后是预期内
 * 状态；发送失败不重试，由 AP-4 校验/反熵补齐（§7）。发送成功即点亮该
 * peer 的健康时间戳（复制交互参与故障检测）。</p>
 */
public class DistroReplicator {

    private static final Logger log = LoggerFactory.getLogger(DistroReplicator.class);

    /**
     * 单批命令条数上限。
     */
    static final int MAX_BATCH_COMMANDS = 128;

    /**
     * 单批估算字节上限（客户端请求体上限默认 1MB，复制批次取其一半留出余量）。
     */
    static final int MAX_BATCH_BYTES = 512 * 1024;

    /**
     * 复制批次发送周期。
     */
    private static final long SYNC_INTERVAL_MILLIS = 100L;

    /**
     * 单次同步调用超时。
     */
    private static final long SYNC_TIMEOUT_MILLIS = 2_000L;

    /**
     * 复制目标 peers（成员表去除本节点；成员静态，构造期定格）。
     */
    private final List<ClusterMember> peers;

    /**
     * 节点间调用客户端（复用 DistroProtocol 的长连接客户端）。
     */
    private final NettyRemotingClient remotingClient;

    /**
     * 同步成功回调（nodeId）：点亮 peer 健康时间戳。
     */
    private final Consumer<String> onPeerAlive;

    /**
     * 待复制队列（FIFO；per-key 版本守卫兜底乱序）。
     */
    private final Queue<CommandRecord> pending = new ConcurrentLinkedQueue<>();

    /**
     * 批次发送线程。
     */
    private final ScheduledExecutorService sender;

    /**
     * 是否已关闭。
     */
    private volatile boolean closed;

    /**
     * 创建复制器。
     *
     * @param assignments 分区归属表（提供成员列表）
     * @param selfNodeId 本节点 ID
     * @param remotingClient 节点间调用客户端
     * @param onPeerAlive 同步成功回调，用于点亮 peer 健康位
     */
    public DistroReplicator(DistroAssignments assignments, String selfNodeId,
                            NettyRemotingClient remotingClient, Consumer<String> onPeerAlive) {
        this.remotingClient = remotingClient;
        this.onPeerAlive = onPeerAlive;
        this.peers = new ArrayList<>();
        for (ClusterMember member : assignments.members()) {
            if (!member.getNodeId().equals(selfNodeId)) {
                this.peers.add(member);
            }
        }
        this.sender = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("poly-config-distro-sync"));
    }

    /**
     * 启动批次发送循环。
     */
    public void start() {
        if (peers.isEmpty()) {
            log.info("复制器启动, peers=0, 单成员集群无须复制");
            return;
        }
        log.info("复制器启动, peers={}", peers);
        sender.scheduleWithFixedDelay(this::syncRound,
                SYNC_INTERVAL_MILLIS, SYNC_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * 入队一条已持久化的本地写（owner 路径）。
     *
     * <p>复制批次携带 owner 分配的版本；命令对象本身不被修改（入队时复制为
     * {@link CommandRecord}）。</p>
     *
     * @param command 已提交的配置命令
     * @param result 提交结果（version 为 owner 分配值）
     */
    public void enqueue(ConfigCommand command, MutationResult result) {
        if (closed || peers.isEmpty()) {
            return;
        }
        CommandRecord record = CommandRecord.from(command);
        record.setVersion(result.getVersion());
        pending.add(record);
    }

    /**
     * 停止复制器。
     */
    public void close() {
        closed = true;
        sender.shutdownNow();
    }

    /**
     * 攒一批命令推送给全部 peers；单个 peer 失败不影响其他 peer。
     *
     * <p>失败不重入队：owner 在乎的只有「已应答的写已持久化」，落后副本
     * 由 AP-4 校验补齐（设计 §7）。整体捕获异常以免调度被取消。</p>
     */
    private void syncRound() {
        try {
            if (pending.isEmpty()) {
                return;
            }
            List<CommandRecord> batch = pollBatch();
            if (batch.isEmpty()) {
                return;
            }
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    ProtocolCodes.DISTRO_SYNC, null);
            request.setBody(JsonUtils.toJson(DistroSyncRequest.from(batch))
                    .getBytes(StandardCharsets.UTF_8));
            for (ClusterMember peer : peers) {
                sendToPeer(peer, request, batch.size());
            }
        } catch (RuntimeException e) {
            log.debug("复制轮次异常", e);
        }
    }

    private void sendToPeer(ClusterMember peer, RemotingCommand request, int batchSize) {
        RemotingCommand response;
        try {
            response = remotingClient.invokeSync(peer.getHost() + ":" + peer.getRemotingPort(),
                    request, SYNC_TIMEOUT_MILLIS);
        } catch (Exception e) {
            log.warn("复制批次发送失败, peer={}, size={}, 等待校验补齐: {}",
                    peer.getNodeId(), batchSize, e.toString());
            return;
        }
        if (response.getCode() != ProtocolCodes.RESPONSE_OK) {
            log.warn("复制批次被 peer 拒绝, peer={}, size={}, code={}, remark={}, 等待校验补齐",
                    peer.getNodeId(), batchSize, response.getCode(), response.getRemark());
            return;
        }
        onPeerAlive.accept(peer.getNodeId());
        if (log.isDebugEnabled()) {
            log.debug("复制批次送达, peer={}, size={}", peer.getNodeId(), batchSize);
        }
    }

    /**
     * 取一批待复制记录：条数与估算字节双上限，至少取一条（大命令单发）。
     */
    private List<CommandRecord> pollBatch() {
        List<CommandRecord> batch = new ArrayList<>();
        long bytes = 0L;
        while (batch.size() < MAX_BATCH_COMMANDS) {
            CommandRecord record = pending.poll();
            if (record == null) {
                break;
            }
            batch.add(record);
            bytes += estimateBytes(record);
            if (bytes >= MAX_BATCH_BYTES) {
                break;
            }
        }
        return batch;
    }

    private static long estimateBytes(CommandRecord record) {
        String content = record.getContent();
        return (content == null ? 0L : content.length()) + 256L;
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger index = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + index.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
