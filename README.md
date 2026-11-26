# 多区域事件接入的灾备提升与租户连续性

租户流用 stream 唯一。position 是一次接受的逻辑位置，epoch 表示实际写主的代次。单一流位置不因切换重用。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **stream**　登记 stream、tenant、primary、secondary 与 policy，policy 为 regional 或 local。
- **write**　在 region 对 stream 写 id 与 digest，引用 epoch。
- **copied**　区域证实 stream/position 与 digest 的实际副本，复制源 source_epoch。
- **promote**　候选 region 试图以 new_epoch 接管。
- **loss**　运营对指定 stream 的 span 缺失授权。 local 模式的未复制尾可以接受明确损失，regional 模式不得以此清理未达约定的已受理写；先补齐或经源端确定显式放弃尚未受理的写。
- **read**　consumer 带 after 与上次所见 epoch 接续。
- **ack**　消费者确认实际处理 through。
- **restart**　重启。
- **inspect**　按 stream 查询。

## 示例

- replication-hole.jsonl
- accepted-tail-and-old-owner.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
