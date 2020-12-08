

CREATE TABLE mrm_first_page(
    PATIENT_SEX_ID VARCHAR,
    DOCTOR3_ZZYS_ID VARCHAR,
    PATIENT_NAME VARCHAR,
    BIRTHPLACE_PROVINCE VARCHAR
)WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='mrm_first_page',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );




 CREATE TABLE gb_t_2260_2007(
    ID VARCHAR,
    S_XZQH_CMC VARCHAR
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='gb_t_2260_2007',
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

-- CREATE TABLE hrm_employee(
--     ID VARCHAR,
--     EMPLOYEE_NAME VARCHAR
--  )WITH(
--     type='kafka10',
--     bootstrapServers='master:9092,slave2:9092,slave3:9092',
--     offsetReset ='earliest',
--     topic ='hrm_employee',
--     timezone='Asia/Shanghai',
--     topicIsPattern ='false',
--     parallelism ='1'
--  );

CREATE TABLE hrm_employee(
    ID VARCHAR,
    EMPLOYEE_NAME VARCHAR,
    PRIMARY KEY (ID),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='oracle',
    url ='jdbc:oracle:thin:@10.158.5.84:1521:dbm',
    userName = 'ogg',
    password = 'ogg',
    tableName = 'HRM_EMPLOYEE',
    --schema = 'dtstack',
    cache = 'LRU',
    asyncPoolSize ='3'
 );


-----------------------------------------------------------------------------------

-- 患者性别分布
-- insert into metric_sink_kfk
-- select
-- 'xb' as PRO_NAME,
-- 'xb_count'as CODE,
-- '' as DT,
-- '性别分布'as LABEL,
-- cast(COUNT(PATIENT_SEX_ID)as string) as DATA,
-- PATIENT_SEX_ID as REMARK
-- from mrm_first_page
-- group by PATIENT_SEX_ID;
--
--
--
--
-- -- 医生治疗患者数
insert into metric_sink_kfk
select * from
(select
'doctor' as PRO_NAME,
'doc_patient_count'as CODE,
'' as DT,
'医生治疗患者数'as LABEL,
cast(COUNT(distinct PATIENT_NAME) as string) DATA,
DOCTOR_NAME as REMARK
from
(
select  e.EMPLOYEE_NAME DOCTOR_NAME,p.PATIENT_NAME from mrm_first_page p
left join hrm_employee e on p.DOCTOR3_ZZYS_ID = e.ID
--where e.EMPLOYEE_NAME is not null
)group by DOCTOR_NAME
) order  by DATA desc limit 10;




-- 患者省份分布
-- insert into metric_sink_kfk
-- select
-- 'province' as PRO_NAME,
-- 'province_patient_count'as CODE,
-- '' as DT,
-- '省份患者分布'as LABEL,
-- cast(count(PATIENT_NAME) as string) DATA,
-- S_XZQH_CMC as REMARK
-- from
-- (select xzq.S_XZQH_CMC,p.PATIENT_NAME from mrm_first_page p
-- left join gb_t_2260_2007 xzq on p.BIRTHPLACE_PROVINCE=xzq.ID
-- where BIRTHPLACE_PROVINCE is not null and xzq.S_XZQH_CMC is not null )
-- group by S_XZQH_CMC;





