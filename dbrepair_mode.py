# dbrepair_mode.py
#
# 总控配置文件：只选择当前 AT 运行模式。
#
# 可选值：
#   "local"   单机 / 本地文件系统模式
#   "cluster" 分布式 / HDFS 文件系统 / HA 自动拉起模式

DBREPAIR_AT_MODE = "local"
