
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

CREATE TABLE metric_side_from_mysql(
    PRO_NAME VARCHAR,
    CODE VARCHAR,
    LABEL VARCHAR,
    DATA VARCHAR,
    DT VARCHAR ,
    REMARK VARCHAR
    PRIMARY KEY(PRO_NAME) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='mysql',
    url ='jdbc:mysql://master:3306/joo?characterEncoding=utf-8&useSSL=false',
    userName ='hive',
    password ='123456',
    tableName ='metric',
    partitionedJoin ='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    asyncPoolSize ='3',
    parallelism ='1'
 );


CREATE TABLE opr_registration_d(
DISCOUNT_AFTER_AMT DECIMAL,
EXEC_DATE TIMESTAMP
 )WITH(
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
    topic ='opr_registration_d',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );
CREATE TABLE opc_drug_presc_d_charge(
        TOTAL_AMT DECIMAL
 )WITH(
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
    topic ='opc_drug_presc_d_charge',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );

 CREATE TABLE opc_diag_service_d_charge(
        TOTAL_AMT DECIMAL,
        EXEC_DATE TIMESTAMP
 )WITH(
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
    topic ='opc_diag_service_d_charge',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );




 CREATE TABLE opc_registration(
    PERSON_INFO_ID varchar
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='opc_registration',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );

CREATE TABLE mrm_first_page_operation (
    ID VARCHAR,
    OPERATION_DATE TIMESTAMP,
    OPERATION_NAME VARCHAR
) WITH (
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
    topic ='mrm_first_page_operation',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
);



--住院收入

CREATE TABLE ipc_drug_presc_d (
    TOTAL_AMT DECIMAL
) WITH (
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
    topic ='ipc_drug_presc_d',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
);



CREATE TABLE ipc_diag_service_d (
    TOTAL_AMT DECIMAL
) WITH (
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
    topic ='ipc_diag_service_d',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
);




 CREATE TABLE ipi_registration(
    PERSON_INFO_ID varchar
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='ipi_registration',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );


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
    PATIENT_SEX_ID VARCHAR,
    DOCTOR3_ZZYS_ID VARCHAR,
    PATIENT_NAME VARCHAR,
    BIRTHPLACE_PROVINCE VARCHAR,
    ID VARCHAR ,
    OUTHOSPITAL_TIME TIMESTAMP,
    OUT_HOSPITAL_WAY VARCHAR
)WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='latest',
    topic ='mrm_first_page',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );

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

--  CREATE TABLE gb_t_2260_2007(
--     ID VARCHAR,
--     S_XZQH_CMC VARCHAR
--  )WITH(
--     type='kafka10',
--     bootstrapServers='master:9092,slave2:9092,slave3:9092',
--     offsetReset ='earliest',
--     topic ='gb_t_2260_2007',
--     timezone='Asia/Shanghai',
--     topicIsPattern ='false',
--     parallelism ='1'
--  );


CREATE TABLE gb_t_2260_2007(
    ID VARCHAR,
    S_XZQH_CMC VARCHAR,
    PRIMARY KEY (ID),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='oracle',
    url ='jdbc:oracle:thin:@10.158.5.84:1521:dbm',
    userName = 'ogg',
    password = 'ogg',
    tableName = 'GB_T_2260_2007',
    cache = 'LRU',
    asyncPoolSize ='3'
 );



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
---------------------------------------------------------------------------------------

--门诊收入
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'opc_fee'as CODE,
concat_ws(':',cast(HOUR(EXEC_DATE) as string),'00') as DT,
'门诊收入'as LABEL,
cast(sum(total_cost) as string) as DATA,
'10' as REMARK from
(select DISCOUNT_AFTER_AMT as total_cost,EXEC_DATE from opr_registration_d
union all
select TOTAL_AMT as total_cost,EXEC_DATE from opc_diag_service_d_charge
union all
select TOTAL_AMT as total_cost, current_timestamp as EXEC_DATE from opc_drug_presc_d_charge
)
group by cast(HOUR(EXEC_DATE) as string)
;




-- 门诊人次
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'opc_diag_count'as CODE,
''as DT,
'门诊人次'as LABEL,
cast(COUNT(DISTINCT PERSON_INFO_ID)as string) as DATA,
'6' as REMARK
from opc_registration;




--手术人数
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'operation_count'as CODE,
'手术例数'as LABEL,
cast(count(DISTINCT ID) as string) as DATA,
DATE_FORMAT(PROCTIME,'yyyy-mm-dd') as DT,
'20' REMARK
FROM mrm_first_page_operation
group by DATE_FORMAT(PROCTIME,'yyyy-mm-dd');



--住院收入
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'ipi_fee'as CODE,
'住院收入'as LABEL,
cast(sum(t.cnt) as string) as DATA,
t.dt as DT,
'5' REMARK
from
(
select sum(TOTAL_AMT)cnt, DATE_FORMAT(PROCTIME,'yyyy-mm-dd') dt from ipc_drug_presc_d
group by DATE_FORMAT(PROCTIME,'yyyy-mm-dd')
union all
select sum(TOTAL_AMT)cnt, DATE_FORMAT(PROCTIME,'yyyy-mm-dd') dt from ipc_diag_service_d
group by DATE_FORMAT(PROCTIME,'yyyy-mm-dd')
)t
group by dt;



-- 住院人次
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'ipi_count'as CODE,
''as DT,
'住院人次'as LABEL,
cast(COUNT(DISTINCT PERSON_INFO_ID)as string) as DATA,
'6' as REMARK
from ipi_registration;





-- 患者省份分布
insert into metric_sink_kfk
select
'province' as PRO_NAME,
'province_patient_count'as CODE,
'' as DT,
'省份患者分布'as LABEL,
cast(count(PATIENT_NAME) as string) DATA,
S_XZQH_CMC as REMARK
from
(select xzq.S_XZQH_CMC,p.PATIENT_NAME from mrm_first_page p
left join gb_t_2260_2007 xzq on p.BIRTHPLACE_PROVINCE=xzq.ID
where BIRTHPLACE_PROVINCE is not null and xzq.S_XZQH_CMC is not null )
group by S_XZQH_CMC;




-- 患者性别分布
insert into metric_sink_kfk
select
'xb' as PRO_NAME,
'xb_count'as CODE,
'' as DT,
'性别分布'as LABEL,
cast(COUNT(PATIENT_SEX_ID)as string) as DATA,
PATIENT_SEX_ID as REMARK
from mrm_first_page
group by PATIENT_SEX_ID;



-- 医生治疗患者数
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
where e.EMPLOYEE_NAME is not null
)group by DOCTOR_NAME
) order  by DATA desc limit 30;







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