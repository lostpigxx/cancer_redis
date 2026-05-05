# test_s15_complex_type_sst_corrupted

## S15：复合类型 SST 损坏流程图

```text
[准备阶段]

开启 heartbeat
  -> 确认所有 partition 正常 opened
  -> 选择一个目标 partition，并记住它当前在哪个 shardsvr 上
  -> 写入 hash / set / zset / list 等复合类型数据
  -> flush 目标 partition，生成复合类型对应的 SST 文件
  -> 确认复合类型数据当前可完整读回
  -> 写入其他 partition 的 guard 数据，并确认这些数据正确


[离线破坏阶段]

临时关闭 heartbeat
  -> 停止目标 partition 所在的 shardsvr
  -> 确认目标 partition 没有被迁移到其他 shardsvr
  -> 选择复合类型数据对应的目标 SST，并损坏 SST 尾部
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
  -> 校验复合类型旧数据：允许丢失；如果 key 还存在，类型和内容必须正确
  -> 向修复后的目标 partition 写入新数据并读回
  -> 确认所有 partition 最终都正常 opened
```
