##记录Binlog同步游标的表
CREATE TABLE binglogDB.t_position(nextposition LONG ,binlogfilename VARCHAR(20))

CREATE EXTERNAL TABLE IF NOT EXISTS TABLE1(
KEYID bigint comment '记录序号',
DDATETIME string comment '数据时间',
DRP double comment 'xxx',
DHP double comment 'xxx',
DVP double comment 'xxx',
YYP double comment 'xxx',
)partitioned by (day int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
location '/DATA/PUBLIC/TABLE1/';