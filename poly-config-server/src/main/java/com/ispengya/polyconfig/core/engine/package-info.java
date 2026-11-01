/**
 * 本地存储引擎。
 *
 * <p>{@link com.ispengya.polyconfig.core.engine.LocalEngine} 封装分段日志、
 * 状态机、组提交与快照，是 standalone 与 AP（owner / 副本路径）共用的底座。
 * 协议层（LocalProtocol / DistroProtocol）负责路由与状态上报，不重复实现存储。</p>
 */
package com.ispengya.polyconfig.core.engine;
