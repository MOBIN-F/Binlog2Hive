##记录Binlog同步游标的表
CREATE TABLE binglogDB.t_position(nextposition LONG ,binlogfilename VARCHAR(20))

CREATE EXTERNAL TABLE IF NOT EXISTS TABLE1(
KEYID bigint comment '记录序号',
DDATETIME string comment '数据时间',
OBTID string comment '站点代码',
OBTNAME string comment '站点名称',
DRP double comment '实时降雨量',
DHP double comment '时累计降雨量',
DVP double comment '日累计降雨量',
YYP double comment '年累计降雨量',
)partitioned by (day int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
location '/DATA/PUBLIC/TABLE1/';