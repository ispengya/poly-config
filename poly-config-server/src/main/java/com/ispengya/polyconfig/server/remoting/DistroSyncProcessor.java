package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.codec.CommandRecord;
import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigOperation;
import com.ispengya.polyconfig.common.protocol.DistroSyncRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.engine.LocalEngine;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * AP 副本侧复制批次处理器（owner → 副本，3xxx 内部协议）。
 *
 * <p>逐条幂等 apply（docs/ap-design.md §3）：本地已有且 version 不小于
 * 来值则丢弃（重复、乱序无害），本地没有且是删除也丢弃（删除不改变现状），
 * 否则走 {@code applyReplicated}——落本地日志、按 owner 分配版本进状态机，
 * 状态机 apply 即触发本地订阅推送（§5「apply 即推送」）。</p>
 */
public class DistroSyncProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(DistroSyncProcessor.class);

    /**
     * 本地存储引擎。
     */
    private final LocalEngine engine;

    /**
     * 创建复制批次处理器。
     *
     * @param engine 本地存储引擎
     */
    public DistroSyncProcessor(LocalEngine engine) {
        this.engine = engine;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            if (request.getBody() == null || request.getBody().length == 0) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "复制批次体为空");
            }
            DistroSyncRequest sync = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8),
                    DistroSyncRequest.class);
            if (sync == null || sync.getCommands() == null) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "复制批次体非法");
            }
            int applied = 0;
            int skipped = 0;
            for (CommandRecord record : sync.getCommands()) {
                if (applyOne(record)) {
                    applied++;
                } else {
                    skipped++;
                }
            }
            if (applied > 0) {
                // 有实际变更才升 INFO：空转批次（重复/过期）保持安静
                log.info("复制批次应用完成, applied={}, skipped={}", applied, skipped);
            } else if (log.isDebugEnabled()) {
                log.debug("复制批次应用完成, applied=0, skipped={}", skipped);
            }
            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            response.setBody(("applied=" + applied + ", skipped=" + skipped)
                    .getBytes(StandardCharsets.UTF_8));
            return response;
        } catch (PolyConfigException e) {
            log.warn("复制批次处理失败: {}", e.getMessage());
            return RemotingCommand.createResponseCommand(
                    toResponseCode(e.getErrorCode()), e.getMessage());
        } catch (RuntimeException e) {
            log.error("复制批次处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "复制批次处理失败");
        }
    }

    /**
     * 应用单条记录。
     *
     * @param record 命令记录（须携带 owner 分配的 version）
     * @return 是否实际应用
     */
    private boolean applyOne(CommandRecord record) {
        ConfigCommand command = record.toCommand();
        Long version = command.getAssignedVersion();
        if (version == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "复制命令缺少 owner 分配版本: " + command.getKey());
        }
        Optional<ConfigEntry> local = engine.read(command.getKey());
        if (local.isPresent() && local.get().getVersion() >= version.longValue()) {
            return false;
        }
        if (!local.isPresent() && command.getOperation() == ConfigOperation.DELETE) {
            // 已删或从未存在：删除不改变现状，跳过保持幂等
            return false;
        }
        engine.applyReplicated(command);
        return true;
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
