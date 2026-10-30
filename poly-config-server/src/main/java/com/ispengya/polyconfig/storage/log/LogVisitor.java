package com.ispengya.polyconfig.storage.log;

/**
 * 日志扫描访问器。
 */
public interface LogVisitor {

    /**
     * 处理一条日志记录。
     *
     * @param physicalOffset 记录在日志中的物理起点
     * @param payload 记录内容
     * @return 返回 false 停止扫描
     */
    boolean onRecord(long physicalOffset, byte[] payload);
}
