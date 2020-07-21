create database if not exist weblog;
use weblog;


drop table if  exists ods_weblog_origin;
create table ods_weblog_origin(
valid string,
remote_addr string,
remote_user string,
time_local string,
request string,
status string,
body_bytes_sent string,
http_referer string,
http_user_agent string)
partitioned by (datestr string)
row format delimited
fields terminated by ',';

drop table if exists ods_click_pageviews;
create table ods_click_pageviews(
session string,
remote_addr string,
remote_user string,
time_local string,
request string,
visit_step string,
page_staylong string,
http_referer string,
http_user_agent string,
body_bytes_sent string,
status string)
partitioned by (datestr string)
row format delimited
fields terminated by ',';

drop table if exists ods_click_stream_visit;
create table ods_click_stream_visit(
session     string,
remote_addr string,
inTime      string,
outTime     string,
inPage      string,
outPage     string,
referal     string,
pageVisits  int)
partitioned by (datestr string)
row format delimited
fields terminated by ',';

drop table if exists t_dim_time;
create table t_dim_time(date_key int,year string,month string,day string,hour string)
row format delimited fields terminated by ',';
