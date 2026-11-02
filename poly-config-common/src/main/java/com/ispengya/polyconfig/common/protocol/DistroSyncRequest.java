package com.ispengya.polyconfig.common.protocol;

import com.ispengya.polyconfig.common.codec.CommandRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * AP 增量复制批次（owner → 副本）。
 *
 * <p>按序携带 owner 已持久化的命令记录，每条记录的 {@code version} 为 owner
 * 分配的 per-key 版本。副本按「比本地新才应用」幂等 apply，重复、乱序无害。
 * 3xxx 段内部协议，不与客户端协议混用。</p>
 */
public class DistroSyncRequest {

    private List<CommandRecord> commands;

    public DistroSyncRequest() {
    }

    /**
     * 从命令记录列表创建复制批次。
     *
     * @param commands 按应用顺序排列的命令记录
     * @return 复制批次
     */
    public static DistroSyncRequest from(List<CommandRecord> commands) {
        DistroSyncRequest request = new DistroSyncRequest();
        request.commands = commands == null ? new ArrayList<CommandRecord>() : commands;
        return request;
    }

    public List<CommandRecord> getCommands() {
        return commands;
    }

    public void setCommands(List<CommandRecord> commands) {
        this.commands = commands;
    }
}
