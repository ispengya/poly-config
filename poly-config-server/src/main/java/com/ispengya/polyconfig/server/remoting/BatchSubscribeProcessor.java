package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.BatchSubscribeRequest;
import com.ispengya.polyconfig.common.protocol.BatchSubscribeResponse;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.protocol.SubscribeItem;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.service.ConfigService;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 批量订阅/对账处理器。
 *
 * <p>一次往返处理多个配置键：客户端为每个键声明持有的 md5/revision，
 * 服务端只返回有变更的视图和已删除的键，未变更的键不出现在响应中。</p>
 */
public class BatchSubscribeProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchSubscribeProcessor.class);

    /**
     * 单次批量请求的最大键数。
     */
    public static final int MAX_TARGETS_PER_BATCH = 1024;

    /**
     * 配置服务。
     */
    private final ConfigService configService;

    /**
     * 创建批量订阅处理器。
     *
     * @param configService 配置服务
     */
    public BatchSubscribeProcessor(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            if (request.getBody() == null || request.getBody().length == 0) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "批量订阅请求体为空");
            }
            BatchSubscribeRequest batch = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8),
                    BatchSubscribeRequest.class);
            List<SubscribeItem> targets = batch == null ? null : batch.getTargets();
            if (targets == null || targets.isEmpty()) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "批量订阅条目为空");
            }
            if (targets.size() > MAX_TARGETS_PER_BATCH) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST,
                        "批量订阅条目数超限: " + targets.size() + " > " + MAX_TARGETS_PER_BATCH);
            }
            BatchSubscribeResponse response = new BatchSubscribeResponse();
            List<ConfigView> changed = new ArrayList<>();
            List<SubscribeItem> deleted = new ArrayList<>();
            for (SubscribeItem target : targets) {
                if (target == null || target.getDataId() == null) {
                    continue;
                }
                Optional<ConfigEntry> entry = configService.get(target.getNamespace(),
                        target.getGroup(), target.getDataId());
                if (entry.isPresent()) {
                    if (!SubscribeProcessor.isUnchanged(
                            target.getVersion(), target.getMd5(), entry.get())) {
                        changed.add(ConfigView.from(entry.get()));
                    }
                } else if (target.getVersion() > 0
                        || (target.getMd5() != null && !target.getMd5().isEmpty())) {
                    // 客户端声明持有但服务端已无此键 → 已删除
                    deleted.add(target);
                }
                // 首次订阅且服务端无此键：无可对账内容，跳过
            }
            response.setChanged(changed);
            response.setDeleted(deleted);
            RemotingCommand responseCommand = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            responseCommand.setBody(JsonUtils.toJson(response)
                    .getBytes(StandardCharsets.UTF_8));
            return responseCommand;
        } catch (PolyConfigException e) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, e.getMessage());
        } catch (RuntimeException e) {
            log.error("批量订阅请求处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "批量订阅处理失败");
        }
    }
}
