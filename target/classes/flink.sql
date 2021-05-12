create table pv(
    pageId VARCHAR,
    userId VARCHAR,
    startTime BIGINT,
    ts as to_timestamp(from_unixtime(startTime))
)with(
'connector'='kafka',
'topic'='pv',
'scan.startup.mode'='latest-offset',
'properties.bootstrap.servers'='localhost:9092',
'format'='json',
'properties.group.id'='flink.test.zz')

create table pv_user_count(
    userId VARCHAR,
    cnt INT,
    ts timestamp(3)
) with (
    'connector' = 'print'
)

