package com.ispengya.polyconfig.core.raft;

/**
 * propose 前置处理（状态线程内按序执行）：校验 + 分配 version + 编码。
 *
 * <p>{@code nextIndex} 是即将追加的 index——per-key version 直接取它
 * （与 AP 的 owner 分配语义一致：version = 写入方全局序号，跨 key 稀疏）。
 * 校验失败抛 {@link com.ispengya.polyconfig.common.error.PolyConfigException}，
 * propose 以该异常完成（HTTP 侧映射 409 等）。</p>
 */
public interface RaftPrepareHandler {

    /**
     * @param nextIndex 即将追加的 index
     * @return 要追加到日志的命令编码字节
     */
    byte[] prepare(long nextIndex);
}
