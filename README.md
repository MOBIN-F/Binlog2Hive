##项目背景
>RDS的数据实时同步到HDFS下，并映射到Hive

##原理
>通过解析RDS的binlog将RDS的增量数据同步到HDFS下，并映射加载到Hive外部分区表

由于RDS表中的第二个字段都为datetime字段，所以刚才以该字段作为Hive的分区字段

###配置文件介绍
* doc/creat table.sql：Hive表的建表语句，除了静态表外，其他全部为天级别外部分区表
* binglog2Hive_conf.properties:里面为所有全部需要同步到HDFS的表
* mysql.properties:Mysql druid连接池配置

###程序说明
binlog解析框架：https://github.com/shyiko/mysql-binlog-connector-java

核心类为BinlogClient
1. 程序主要序列化以下几个事件
* TABLE_MAP：包括表名，数据库名
* WRITE_ROWS：包含增量的业务记录

2. 
* 首先对TABLE_MAP事件进行序列化，再结合binlog2Hive_conf.propertiesd配置过滤出我们需要同步的表再对WRITE_ROW事件进行序列化
* 解析WRITE_ROWS时，将</DATA/PUBLIC/表名，记录>存储到ConcurrentHashMap<String, List<Serializable[]>> mapA中
* 解析的记录超过一定阀值option.countInterval后再统一写HDFS文件
* 写HDFS文件时，遍历mapA，根据表名分类，整理成</DATA/PUBLIC/表名/day=xxx,记录>存储到
ConcurrentHashMap<String, ArrayList<Serializable[]>> mapB，最后再统一遍历mapB将数据写入到HDFS，写到哪个文件中是根据mapB的key来确定的
* 文件操作类在FSUtils中，写文件时以下三种情况
  1. 如果目录不存在就创建文件并将Hive表的分区映射到这个路径下，
  2. 文件已存在且文件大小小于250MB就以追加的方式写文件
  3. 文件大小超250MB就重新写成另一个新文件，以HDFS_BLOCK_SIZE为标准

**项目已经去掉敏感业务信息**



