package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigEntry;
import com.ispengya.polyconfig.common.model.ConfigKey;
import com.ispengya.polyconfig.common.protocol.ConfigView;
import com.ispengya.polyconfig.common.protocol.FetchRequest;
import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.consistency.ReadConsistency;
import com.ispengya.polyconfig.core.service.ConfigService;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
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
                    key.getGroup(), key.getDataId(), parseConsistency(fetch.getConsistency()));
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
            // 选举中/非 leader 等暂态错误 → 500 类可重试（客户端换节点重试），
            // 其余（参数非法等）→ 400 不重试
            boolean retryable = e.getErrorCode() == ErrorCode.UNAVAILABLE
                    || e.getErrorCode() == ErrorCode.NOT_LEADER;
            return RemotingCommand.createResponseCommand(
                    retryable ? ProtocolCodes.RESPONSE_SERVER_ERROR
                            : ProtocolCodes.RESPONSE_BAD_REQUEST,
                    e.getMessage());
        } catch (RuntimeException e) {
            log.error("拉取请求处理失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "拉取处理失败");
        }
    }

    /**
     * 解析 FetchRequest 的一致性级别（null/未知 = LOCAL，兼容旧客户端）。
     *
     * @param value 协议携带的级别名
     * @return 读一致性级别
     */
    private static ReadConsistency parseConsistency(String value) {
        if (value == null || value.trim().isEmpty()) {
            return ReadConsistency.LOCAL;
        }
        try {
            return ReadConsistency.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "不支持的 consistency 取值: " + value);
        }
    }
}
