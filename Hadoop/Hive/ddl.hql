drop table if exists ip_regions;
create external table ip_regions (
        ip string,
        region string
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/data/user_logs/ip_data_M';


drop table if exists users;
create external table users (
 ip string,
 browser string,
 sex string,
 age int
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/data/user_logs/user_data_M';



add jar /usr/local/hive/lib/hive-serde.jar;
drop table if exists logs_raw;

create external table logs_raw
 (
 ip string,
 `date` string,
 request string,
 page_size int,
 http_status int,
 user_agent string
)
row format
serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties("input.regex" = "^([^\\t]*)\\t{3}([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t([^\\t]*)\\t(\\w+/\\d+\\.\\d+).*$")
stored as textfile
location '/data/user_logs/user_logs_M';



drop table if exists logs;

create external table logs
 (
 ip string,
 request string,
 page_size int,
 http_status int,
 user_agent string
) partitioned by (`date` string);


set hive.exec.max.dynamic.partitions.pernode=116;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table logs partition(`date`) select ip, request, page_size, http_status, user_agent, from_unixtime(unix_timestamp(`date`, "yyyyMMddHHmmss"), "yyyy-MM-dd") as `date` from logs_raw from logs_raw;
