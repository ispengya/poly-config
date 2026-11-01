/**
 * AP（Distro）一致性协议实现。
 *
 * <p>全量副本 + 分片写责任：分区（namespace + group）归属由静态成员列表
 * 推导，owner 是分区唯一写入口；读与推送本地化，节点间通过异步复制与
 * 周期校验收敛。设计文档见 docs/ap-design.md。</p>
 *
 * <p>实施顺序：AP-1 成员与骨架 → AP-2 写转发 → AP-3 异步复制与副本推送
 * → AP-4 校验/反熵与重启收敛 → AP-5 集群测试与控制台集群页。</p>
 */
package com.ispengya.polyconfig.core.distro;
