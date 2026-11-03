package com.ispengya.polyconfig.core.distro;

import com.ispengya.polyconfig.common.codec.CommandRecord;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.DistroSyncRequest;
import com.ispengya.polyconfig.common.protocol.DistroVerifyRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.VerifyEntry;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.polyconfig.core.engine.LocalEngine;
import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 周期校验器（docs/ap-design.md §6）。
 *
 * <p>每轮向每个成员发送本节点持有的、该成员拥有的分区全量摘要
 * （key→version，含墓碑）；应答体携带 owner 侧增量，按与
 * {@code DISTRO_SYNC} 相同的幂等规则应用——apply 即触发本地推送。
 * 发送失败仅告警不重试，下一轮自动补（校验周期即收敛上界）。</p>
 *
 * <p>校验同时就是探活（§6/§7）：应答成功点亮该 peer 的健康时间戳，
 * 过渡期的纯展示 TCP 探测随之移除。空集群无分区也照样发送摘要
 * （空表往返），健康位不依赖是否有数据。</p>
 */
public class DistroVerifier {

    private static final Logger log = LoggerFactory.getLogger(DistroVerifier.class);

    /**
     * 单次校验调用超时。
     */
    private static final long VERIFY_TIMEOUT_MILLIS = 3_000L;

    /**
     * 分区归属表（提供成员列表与分区归属）。
     */
    private final DistroAssignments assignments;

    /**
     * 校验目标 peers（成员表去除本节点；成员静态，构造期定格）。
     */
    private final List<ClusterMember> peers;

    /**
     * 本节点 ID。
     */
    private final String selfNodeId;

    /**
     * 本地存储引擎（读摘要、应用增量）。
     */
    private final LocalEngine engine;

    /**
     * 节点间调用客户端（复用 DistroProtocol 的长连接客户端）。
     */
    private final NettyRemotingClient remotingClient;

    /**
     * 校验成功回调（nodeId）：点亮 peer 健康时间戳。
     */
    private final Consumer<String> onPeerAlive;

    /**
     * 校验周期（毫秒）。
     */
    private final long intervalMillis;

    /**
     * 校验调度线程。
     */
    private final ScheduledExecutorService scheduler;

    /**
     * 是否已关闭。
     */
    private volatile boolean closed;

    /**
     * 创建校验器。
     *
     * @param assignments 分区归属表
     * @param selfNodeId 本节点 ID
     * @param engine 本地存储引擎
     * @param remotingClient 节点间调用客户端
     * @param intervalMillis 校验周期（毫秒）
     * @param onPeerAlive 校验成功回调，用于点亮 peer 健康位
     */
    public DistroVerifier(DistroAssignments assignments, String selfNodeId, LocalEngine engine,
                          NettyRemotingClient remotingClient, long intervalMillis,
                          Consumer<String> onPeerAlive) {
        this.assignments = assignments;
        this.selfNodeId = selfNodeId;
        this.engine = engine;
        this.remotingClient = remotingClient;
        this.intervalMillis = intervalMillis;
        this.onPeerAlive = onPeerAlive;
        this.peers = new ArrayList<>();
        for (ClusterMember member : assignments.members()) {
            if (!member.getNodeId().equals(selfNodeId)) {
                this.peers.add(member);
            }
        }
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("poly-config-distro-verify"));
    }

    /**
     * 启动校验循环（首轮尽快执行，保证重启后快速收敛）。
     */
    public void start() {
        if (peers.isEmpty()) {
            log.info("校验器启动, peers=0, 单成员集群无须校验");
            return;
        }
        log.info("校验器启动, interval={}ms, peers={}", intervalMillis, peers.size());
        long initialDelay = Math.min(intervalMillis, 1_000L);
        scheduler.scheduleWithFixedDelay(this::verifyRound,
                initialDelay, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 停止校验器。
     */
    public void close() {
        closed = true;
        scheduler.shutdownNow();
    }

    /**
     * 一轮校验：向每个成员发送摘要并应用应答增量；单个成员失败不影响其他成员。
     *
     * <p>整体按成员隔离捕获异常：调度任务抛出会被
     * {@code scheduleWithFixedDelay} 取消后续执行，校验绝不能中断。</p>
     */
    private void verifyRound() {
        if (closed) {
            return;
        }
        for (ClusterMember peer : peers) {
            try {
                verifyPeer(peer);
            } catch (RuntimeException e) {
                log.warn("校验轮次异常, peer={}", peer.getNodeId(), e);
            }
        }
    }

    private void verifyPeer(ClusterMember peer) {
        List<VerifyEntry> entries = new ArrayList<>();
        for (VerifyEntry entry : engine.verifyTable()) {
            ConfigKey key = new ConfigKey(entry.getNamespace(), entry.getGroup(),
                    entry.getDataId());
            if (peer.getNodeId().equals(assignments.ownerOf(key).getNodeId())) {
                entries.add(entry);
            }
        }
        DistroVerifyRequest verify = DistroVerifyRequest.of(selfNodeId, entries);
        RemotingCommand request = RemotingCommand.createRequestCommand(
                ProtocolCodes.DISTRO_VERIFY, null);
        request.setBody(JsonUtils.toJson(verify).getBytes(StandardCharsets.UTF_8));
        RemotingCommand response;
        try {
            response = remotingClient.invokeSync(
                    peer.getHost() + ":" + peer.getRemotingPort(), request,
                    VERIFY_TIMEOUT_MILLIS);
        } catch (Exception e) {
            log.warn("校验发送失败, peer={}, entries={}, 下一轮重试: {}",
                    peer.getNodeId(), entries.size(), e.toString());
            return;
        }
        if (response.getCode() != ProtocolCodes.RESPONSE_OK) {
            log.warn("校验被拒绝, peer={}, code={}, remark={}",
                    peer.getNodeId(), response.getCode(), response.getRemark());
            return;
        }
        // 应答成功即证明对端存活：点亮健康位（探活寄生在校验上，§6）
        onPeerAlive.accept(peer.getNodeId());
        int applied = 0;
        int skipped = 0;
        if (response.getBody() != null && response.getBody().length > 0) {
            DistroSyncRequest delta = JsonUtils.parseObject(
                    new String(response.getBody(), StandardCharsets.UTF_8),
                    DistroSyncRequest.class);
            if (delta != null && delta.getCommands() != null) {
                for (CommandRecord record : delta.getCommands()) {
                    if (engine.applyReplicatedIfNewer(record)) {
                        applied++;
                    } else {
                        skipped++;
                    }
                }
            }
        }
        if (applied > 0) {
            log.info("校验补齐, peer={}, applied={}, skipped={}", peer.getNodeId(), applied, skipped);
        } else if (log.isDebugEnabled()) {
            log.debug("校验一致, peer={}, entries={}", peer.getNodeId(), entries.size());
        }
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
