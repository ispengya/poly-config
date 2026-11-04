package com.ispengya.polyconfig.server.remoting;

import com.ispengya.polyconfig.common.protocol.ProtocolCodes;
import com.ispengya.polyconfig.common.util.JsonUtils;
import com.ispengya.polyconfig.consistency.NodeDetail;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * AP 状态采集处理器（3xxx 内部协议，设计 §4"内部通道的第四种用途"）。
 *
 * <p>应答本节点的详细状态（自报视图：拥有的分区、校验/健康时间戳、
 * 校验异常统计），由控制台所在节点向全体成员收集后聚合展示。
 * 请求体不需要内容——采集是纯读操作。</p>
 */
public class DistroStatusProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(DistroStatusProcessor.class);

    /**
     * 本节点详细状态供给者（由 DistroProtocol 提供，避免反向依赖）。
     */
    private final Supplier<NodeDetail> detailSupplier;

    /**
     * 创建状态采集处理器。
     *
     * @param detailSupplier 本节点详细状态供给者
     */
    public DistroStatusProcessor(Supplier<NodeDetail> detailSupplier) {
        this.detailSupplier = detailSupplier;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            NodeDetail detail = detailSupplier.get();
            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_OK, "ok");
            response.setBody(JsonUtils.toJson(detail).getBytes(StandardCharsets.UTF_8));
            return response;
        } catch (RuntimeException e) {
            log.error("状态采集应答失败", e);
            return RemotingCommand.createResponseCommand(
                    ProtocolCodes.RESPONSE_SERVER_ERROR, "状态采集失败");
        }
    }
}
