package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.codec.CommandRecord;
import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.model.HistoryEntry;
import com.ispengya.polyconfig.common.protocol.DistroSyncRequest;
import com.ispengya.polyconfig.common.protocol.DistroVerifyRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.VerifyEntry;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.distro.DistroAssignments;
import com.ispengya.polyconfig.core.engine.LocalEngine;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * AP 周期校验处理器（副本 → owner，3xxx 内部协议，docs/ap-design.md §6）。
 *
 * <p>接收副本的全量分区摘要（key→version，含墓碑），与本节点拥有的分区
 * 逐 key 比对：</p>
 * <ul>
 *   <li>owner 更新 / 副本缺失 → 应答增量（携带内容的 {@link CommandRecord}，
 *       副本按与 {@code DISTRO_SYNC} 相同的幂等规则应用并触发本地推送）；</li>
 *   <li>副本持有本节点从未有过的活键、或版本反超 → 记 WARN 异常
 *       （owner 是唯一写入口，出现即数据面异常）；</li>
 *   <li>双方一致 → 空增量应答（平时一轮只有几百字节往返）。</li>
 * </ul>
 *
 * <p>设计中的 DISTRO_PULL（owner → 副本增量拉取）折叠进本应答体：
 * "应答 VERIFY 即增量"，按需主动拉取留待后续场景再启用。</p>
 *
 * <p>每个节点都注册本处理器，但只比对/应答自己拥有的分区——副本按目标
 * 成员过滤摘要后发送，双保险。</p>
 */
public class DistroVerifyProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(DistroVerifyProcessor.class);

    /**
     * 单次校验应答的增量条数上限（超出部分下一轮校验补齐）。
     */
    static final int MAX_DELTA_COMMANDS = 128;

    /**
     * 单次校验应答的增量估算字节上限（客户端请求体上限默认 1MB，取其一半）。
     */
    static final long MAX_DELTA_BYTES = 512 * 1024;

    /**
     * 本地存储引擎。
     */
    private final LocalEngine engine;

    /**
     * 分区归属表。
     */
    private final DistroAssignments assignments;

    /**
     * 本节点 ID（只应答本节点拥有的分区）。
     */
    private final String selfNodeId;

    /**
     * 创建校验处理器。
     *
     * @param engine 本地存储引擎
     * @param assignments 分区归属表
     * @param selfNodeId 本节点 ID
     */
    public DistroVerifyProcessor(LocalEngine engine, DistroAssignments assignments,
                                 String selfNodeId) {
        this.engine = engine;
        this.assignments = assignments;
        this.selfNodeId = selfNodeId;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            if (request.getBody() == null || request.getBody().length == 0) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "校验请求体为空");
            }
            DistroVerifyRequest verify = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8),
                    DistroVerifyRequest.class);
            if (verify == null) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "校验请求体非法");
            }
            // 两侧索引都只保留本节点拥有的分区（发送方已过滤，这里兜底）
            Map<ConfigKey, VerifyEntry> own = new HashMap<>();
            for (VerifyEntry entry : engine.verifyTable()) {
                ConfigKey key = keyOf(entry);
                if (assignments.ownerOf(key).getNodeId().equals(selfNodeId)) {
                    own.put(key, entry);
                }
            }
            Map<ConfigKey, VerifyEntry> theirs = new HashMap<>();
            if (verify.getEntries() != null) {
                for (VerifyEntry entry : verify.getEntries()) {
                    if (entry == null || entry.getDataId() == null) {
                        continue;
                    }
                    ConfigKey key = keyOf(entry);
                    if (assignments.ownerOf(key).getNodeId().equals(selfNodeId)) {
                        theirs.put(key, entry);
                    }
                }
            }
            List<CommandRecord> delta = new ArrayList<>();
            long bytes = 0L;
            // owner 表逐键比对：覆盖副本缺失 / 落后 / 墓碑未同步三类缺口
            for (Map.Entry<ConfigKey, VerifyEntry> entry : own.entrySet()) {
                CommandRecord record = diff(entry.getKey(), theirs.get(entry.getKey()),
                        entry.getValue(), verify.getSourceNodeId());
                if (record == null) {
                    continue;
                }
                bytes += estimateBytes(record);
                delta.add(record);
                if (delta.size() >= MAX_DELTA_COMMANDS || bytes >= MAX_DELTA_BYTES) {
                    log.info("校验增量达到单次上限，剩余部分下一轮补齐, replica={}, batchSize={}",
                            verify.getSourceNodeId(), delta.size());
                    break;
                }
            }
            // 副本表中本节点从未有过的键：owner 是唯一写入口，出现即异常
            for (Map.Entry<ConfigKey, VerifyEntry> entry : theirs.entrySet()) {
                if (!own.containsKey(entry.getKey()) && !entry.getValue().isDeleted()) {
                    log.warn("校验异常: 副本持有本节点从未有过的键, replica={}, key={}, version={}",
                            verify.getSourceNodeId(), entry.getKey().toPath(),
                            entry.getValue().getVersion());
                }
            }
            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            response.setBody(JsonUtils.toJson(DistroSyncRequest.from(delta))
                    .getBytes(StandardCharsets.UTF_8));
            return response;
        } catch (PolyConfigException e) {
            log.warn("校验处理失败: {}", e.getMessage());
            return RemotingCommand.createResponseCommand(
                    toResponseCode(e.getErrorCode()), e.getMessage());
        } catch (RuntimeException e) {
            log.error("校验处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "校验处理失败");
        }
    }

    /**
     * 逐 key 比对并决定是否下发增量。
     *
     * <p>比较只信任 owner 侧的版本：副本摘要里的墓碑版本是其本地序号，
     * 只用于表达"已删除"，不参与大小比较。{@code theirs} 为 {@code null}
     * 表示副本完全缺失该键。</p>
     *
     * @param key 配置键
     * @param theirs 副本侧摘要；{@code null} 表示副本没有该键
     * @param mine owner 侧摘要
     * @param replicaId 副本节点 ID（异常日志用）
     * @return 需下发的增量记录；{@code null} 表示无须下发
     */
    private CommandRecord diff(ConfigKey key, VerifyEntry theirs, VerifyEntry mine,
                               String replicaId) {
        if (mine.isDeleted()) {
            if (theirs == null || theirs.isDeleted()) {
                // 副本已无该键（或同样已删），收敛完成（副本侧墓碑版本不比较）
                return null;
            }
            if (theirs.getVersion() < mine.getVersion()) {
                return tombstoneRecord(key);
            }
            log.warn("校验异常: 副本版本不小于 owner 墓碑, replica={}, key={}, "
                            + "replicaVersion={}, ownerVersion={}",
                    replicaId, key.toPath(), theirs.getVersion(), mine.getVersion());
            return null;
        }
        // owner 持有活条目
        if (theirs == null) {
            return liveRecord(key);
        }
        if (theirs.isDeleted()) {
            // 副本误报已删（或删除后未跟上重新发布）：owner 是真相源，补发活数据
            return liveRecord(key);
        }
        if (theirs.getVersion() > mine.getVersion()) {
            log.warn("校验异常: 副本版本反超 owner, replica={}, key={}, "
                            + "replicaVersion={}, ownerVersion={}",
                    replicaId, key.toPath(), theirs.getVersion(), mine.getVersion());
            return null;
        }
        if (theirs.getVersion() < mine.getVersion()) {
            return liveRecord(key);
        }
        return null;
    }

    /**
     * 构造活条目增量（操作取最新历史，保持各节点历史语义一致）。
     *
     * @param key 配置键
     * @return 命令记录；条目恰好并发变更时返回 {@code null}，下一轮再补
     */
    private CommandRecord liveRecord(ConfigKey key) {
        Optional<ConfigEntry> entry = engine.read(key);
        if (!entry.isPresent()) {
            // 比对快照后恰好被并发删除：跳过，下一轮按墓碑路径处理
            return null;
        }
        ConfigOperation operation = ConfigOperation.PUBLISH;
        List<HistoryEntry> history = engine.history(key, 1);
        if (!history.isEmpty() && history.get(0).getOperation() != null) {
            operation = history.get(0).getOperation();
        }
        if (operation == ConfigOperation.DELETE) {
            // 历史头已是删除而当前表仍有值：并发变更窗口，下一轮再比
            return null;
        }
        ConfigEntry current = entry.get();
        CommandRecord record = new CommandRecord();
        record.setOperation(operation.name());
        record.setNamespace(key.getNamespace());
        record.setGroup(key.getGroup());
        record.setDataId(key.getDataId());
        record.setContent(current.getContent());
        record.setType(current.getType().name());
        record.setOperator(current.getOperator());
        record.setTimestamp(current.getModifiedTime());
        record.setVersion(current.getVersion());
        return record;
    }

    /**
     * 构造墓碑增量（版本取最新删除历史的 revision，即 owner 分配版本）。
     *
     * @param key 配置键
     * @return 命令记录；状态恰好又变化时返回 {@code null}，下一轮再比
     */
    private CommandRecord tombstoneRecord(ConfigKey key) {
        if (engine.read(key).isPresent()) {
            // 快照后又被重新发布：补最新活数据
            return liveRecord(key);
        }
        List<HistoryEntry> history = engine.history(key, 1);
        HistoryEntry head = history.isEmpty() ? null : history.get(0);
        if (head == null || head.getOperation() != ConfigOperation.DELETE) {
            // 历史头已不是删除：状态又变了，下一轮再比
            return null;
        }
        CommandRecord record = new CommandRecord();
        record.setOperation(ConfigOperation.DELETE.name());
        record.setNamespace(key.getNamespace());
        record.setGroup(key.getGroup());
        record.setDataId(key.getDataId());
        record.setContent("");
        record.setType(head.getType() == null ? "TEXT" : head.getType().name());
        record.setOperator(head.getOperator());
        record.setTimestamp(head.getTimestamp());
        record.setVersion(head.getRevision());
        return record;
    }

    private static ConfigKey keyOf(VerifyEntry entry) {
        return new ConfigKey(entry.getNamespace(), entry.getGroup(), entry.getDataId());
    }

    private static long estimateBytes(CommandRecord record) {
        String content = record.getContent();
        return (content == null ? 0L : content.length()) + 256L;
    }

    private static int toResponseCode(ErrorCode errorCode) {
        switch (errorCode) {
            case INVALID_ARGUMENT:
            case PAYLOAD_TOO_LARGE:
                return ProtocolCodes.RESPONSE_BAD_REQUEST;
            default:
                return ProtocolCodes.RESPONSE_SERVER_ERROR;
        }
    }
}
