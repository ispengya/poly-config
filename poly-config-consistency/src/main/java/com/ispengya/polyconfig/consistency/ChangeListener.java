package com.ispengya.polyconfig.consistency;

import com.ispengya.polyconfig.common.model.ConfigChangeEvent;

/**
 * 配置变更监听器。
 *
 * <p>状态机每次应用命令后触发，用于驱动服务端推送和客户端回调。</p>
 */
public interface ChangeListener {

    /**
     * 处理配置变更事件。
     *
     * @param event 变更事件
     */
    void onChange(ConfigChangeEvent event);
}
