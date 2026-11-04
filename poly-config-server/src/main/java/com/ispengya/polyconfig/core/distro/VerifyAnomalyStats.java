package com.ispengya.polyconfig.core.distro;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 校验异常统计（docs/ap-design.md §6："对方有我没有的 → 异常，报警"）。
 *
 * <p>异常只报警不自动修复，本计数器把"翻日志才能发现"变成状态接口可查——
 * 控制台集群页据此亮异常徽标。线程安全：计数原子、详情 volatile 写入。</p>
 */
public class VerifyAnomalyStats {

    /**
     * 累计异常次数。
     */
    private final AtomicLong count = new AtomicLong();

    /**
     * 最近一次异常时间（epoch millis，0 表示从未）。
     */
    private volatile long lastTime;

    /**
     * 最近一次异常描述。
     */
    private volatile String lastDetail = "";

    /**
     * 记录一次校验异常。
     *
     * @param detail 异常描述（键、版本、来源副本）
     */
    public void record(String detail) {
        count.incrementAndGet();
        lastDetail = detail == null ? "" : detail;
        lastTime = System.currentTimeMillis();
    }

    public long getCount() {
        return count.get();
    }

    public long getLastTime() {
        return lastTime;
    }

    public String getLastDetail() {
        return lastDetail;
    }
}
