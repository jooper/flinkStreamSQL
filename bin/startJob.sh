#!/usr/bin/env bash
#
#sh -x submit.sh \
#-mode local \
#-sql /home/flink/jobs/${1} \
#-name $2 \
#-localSqlPluginPath /home/flink/sqlplugins \
#-mode standalone \
#-flinkconf /home/flink/flink-1.10.2/conf \
#-remoteSqlPluginPath /flinkStreamPlugins
#




#sh submit.sh \
#  -mode standalone \
#  -sql /home/flink/jobs/${1} \
#  -name $2 \
#  -localSqlPluginPath /home/flink/sqlplugins \
#  -remoteSqlPluginPath /flinkStreamPlugins \
#  -flinkconf /home/flink/flink-1.10.2/conf \
#  -flinkJarPath /home/flink/flink-1.10.2/lib \
#  -pluginLoadMode shipfile



#--ok
#--use case
#--./startJob.sh xxxx.sql  metric_name

sh submit.sh \
  -mode standalone \
  -sql /home/flink/jobs/${1} \
  -name $2 \
  -localSqlPluginPath /home/flink/sqlplugins \
  -remoteSqlPluginPath /home/flink/sqlplugins \
  -flinkconf /home/flink/flink-1.10.2/conf \
  -flinkJarPath /home/flink/flink-1.10.2/lib \
