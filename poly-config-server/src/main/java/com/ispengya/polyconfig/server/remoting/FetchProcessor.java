package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.FetchRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.core.service.ConfigService;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * 配置拉取处理器。
 */
public class FetchProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(FetchProcessor.class);

    /**
     * 配置服务。
     */
    private final ConfigService configService;

    /**
     * 创建拉取处理器。
     *
     * @param configService 配置服务
     */
    public FetchProcessor(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            if (request.getBody() == null || request.getBody().length == 0) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_BAD_REQUEST, "拉取请求体为空");
            }
            FetchRequest fetch = JsonUtils.parseObject(
                    new String(request.getBody(), StandardCharsets.UTF_8), FetchRequest.class);
            ConfigKey key = new ConfigKey(fetch.getNamespace(), fetch.getGroup(), fetch.getDataId());
            Optional<ConfigEntry> entry = configService.get(key.getNamespace(),
                    key.getGroup(), key.getDataId());
            if (!entry.isPresent()) {
                return RemotingCommand.createResponseCommand(
                        ProtocolCodes.RESPONSE_NOT_FOUND, "配置不存在");
            }
            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            response.setBody(JsonUtils.toJson(ConfigView.from(entry.get()))
                    .getBytes(StandardCharsets.UTF_8));
            return response;
        } catch (PolyConfigException e) {
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_BAD_REQUEST, e.getMessage());
        } catch (RuntimeException e) {
            log.error("拉取请求处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "拉取处理失败");
        }
    }
}
