# test_w3_wal_middle_loss_open_fail_repair

## W3：WAL 中间丢失导致 Open 失败流程图

```text
[准备阶段]

开启 heartbeat
  -> 确认所有 partition 正常 opened
  -> 选择一个目标 partition，并记住它当前在哪个 shardsvr 上
  -> 记录目标 partition 写入前的 WAL 快照
  -> 向目标 partition 写入 WAL 负载数据，并确认 key 确实路由到目标 partition
  -> 记录目标 partition 写入后的 WAL 快照
  -> 写入其他 partition 的 guard 数据，并确认这些数据正确


[离线破坏阶段]

临时关闭 heartbeat
  -> 停止目标 partition 所在的 shardsvr
  -> 确认目标 partition 没有被迁移到其他 shardsvr
  -> 只对写入后确实增长过的 WAL 注入中间丢失
  -> 在原位置重新启动这个 shardsvr
  -> 恢复 heartbeat


[故障确认阶段]

等待目标 partition 被识别为 corrupted
  -> 确认坏掉的 partition 仍留在原 shardsvr 上，没有迁移
  -> 确认其他 partition 没受影响，guard 数据仍然正确


[修复验证阶段]

对目标 partition 执行自动修复
  -> 等待目标 partition 恢复 opened
  -> 再次确认目标 partition 仍在原 shardsvr 上
  -> 再次确认其他 partition 的 guard 数据完整
  -> 校验 WAL-only 旧数据：允许丢失；如果还能读到，值必须正确
  -> 向修复后的目标 partition 写入新数据并读回
  -> 确认所有 partition 最终都正常 opened
```
