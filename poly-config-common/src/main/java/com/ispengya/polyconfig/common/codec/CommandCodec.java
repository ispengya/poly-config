package com.ispengya.polyconfig.common.codec;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.model.ConfigCommand;
import com.ispengya.polyconfig.common.util.JsonUtils;

import java.nio.charset.StandardCharsets;

/**
 * 配置命令日志编解码。
 *
 * <p>日志中的命令必须能独立回放，编码结果不依赖调用方上下文。</p>
 */
public final class CommandCodec {

    private CommandCodec() {
    }

    /**
     * 将配置命令编码为日志 payload。
     *
     * @param command 配置命令
     * @return UTF-8 JSON 字节
     */
    public static byte[] encode(ConfigCommand command) {
        return JsonUtils.toJson(CommandRecord.from(command)).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 从日志 payload 解码配置命令。
     *
     * @param payload 日志内容
     * @return 配置命令
     */
    public static ConfigCommand decode(byte[] payload) {
        if (payload == null) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "日志记录为空");
        }
        try {
            CommandRecord record = JsonUtils.parseObject(
                    new String(payload, StandardCharsets.UTF_8), CommandRecord.class);
            if (record == null) {
                throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "日志命令为空");
            }
            return record.toCommand();
        } catch (PolyConfigException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new PolyConfigException(ErrorCode.STORAGE_ERROR, "日志命令解析失败", e);
        }
    }
}
