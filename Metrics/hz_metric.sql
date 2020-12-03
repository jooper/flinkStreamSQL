 CREATE TABLE ipd_consult_apply(
    ID VARCHAR ,
    REQ_DATE TIMESTAMP
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='ipd_consult_apply',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );


  CREATE TABLE mrm_first_page(
    ID VARCHAR ,
    OUTHOSPITAL_TIME TIMESTAMP,
    OUT_HOSPITAL_WAY VARCHAR
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='mrm_first_page',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );

  CREATE TABLE mtw_lab_h(
    ID VARCHAR ,
    REQ_DATE TIMESTAMP
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='mtw_lab_h',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );


  CREATE TABLE mtw_exam_h(
    ID VARCHAR ,
    REQ_DATE TIMESTAMP
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='mtw_exam_h',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );


 CREATE TABLE metric_sink_kfk (
    PRO_NAME VARCHAR,
    CODE VARCHAR,
    LABEL VARCHAR,
    DATA VARCHAR,
    DT VARCHAR ,
    REMARK VARCHAR
) WITH (
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    topic='hospital_metric',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    updateMode='upsert',
    parallelism ='1'
);

-- 会诊
insert into metric_sink_kfk
select
'hz' as PRO_NAME,
'hz_count'as CODE,
concat_ws(':',cast(HOUR(REQ_DATE) as string),'00') as DT,
'会诊'as LABEL,
-- cast(COUNT(DISTINCT ID)as string) as DATA,
cast(COUNT(ID)as string) as DATA,
'6' as REMARK
from (select * from ipd_consult_apply order by HOUR(REQ_DATE))
group by concat_ws(':',cast(HOUR(REQ_DATE) as string),'00');




--转诊
insert into metric_sink_kfk
select
'zz' as PRO_NAME,
'zz_count'as CODE,
concat_ws(':',cast(HOUR(OUTHOSPITAL_TIME) as string),'00') as DT,
'转诊'as LABEL,
cast(COUNT(ID)as string) as DATA,
'66' as REMARK
from (select * from mrm_first_page
--where OUT_HOSPITAL_WAY in ('2','3')
order by HOUR(OUTHOSPITAL_TIME))
group by concat_ws(':',cast(HOUR(OUTHOSPITAL_TIME) as string),'00');


--检查
insert into metric_sink_kfk
select
'jc' as PRO_NAME,
'jc_count'as CODE,
concat_ws(':',cast(HOUR(REQ_DATE) as string),'00') as DT,
'检查'as LABEL,
cast(COUNT(ID)as string) as DATA,
'66' as REMARK
from (
select * from mtw_exam_h order by HOUR(REQ_DATE)
)
group by concat_ws(':',cast(HOUR(REQ_DATE) as string),'00');




--检验
insert into metric_sink_kfk
select
'jy' as PRO_NAME,
'jy_count'as CODE,
concat_ws(':',cast(HOUR(REQ_DATE) as string),'00') as DT,
'检验'as LABEL,
cast(COUNT(ID)as string) as DATA,
'66' as REMARK
from (
select * from mtw_lab_h order by HOUR(REQ_DATE)
)
group by concat_ws(':',cast(HOUR(REQ_DATE) as string),'00');