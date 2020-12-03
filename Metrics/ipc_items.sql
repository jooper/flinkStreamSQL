CREATE TABLE ipc_drug_presc_h(
    ID STRING,
	HEALTH_SERVICE_ORG_ID STRING,
	CHARGE_DATE  STRING
 )WITH(
    type ='kafka11',
    bootstrapServers ='slave2:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='ipc_drug_presc_h',
    groupId='CDXT',
    parallelism ='1',
    timezone='Asia/Shanghai',
    sourcedatatype ='json'
 );
 CREATE TABLE ipc_drug_presc_d(
    DRUG_PRESC_H_ID  STRING,
    TOTAL_AMT DECIMAL
 )WITH(
    type ='kafka11',
    bootstrapServers ='slave2:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='ipc_drug_presc_d',
    groupId='CDXT',
    parallelism ='1',
    timezone='Asia/Shanghai',
    sourcedatatype ='json'
 );

 CREATE TABLE ipc_diag_service_h(
    ID STRING,
	HEALTH_SERVICE_ORG_ID STRING,
	CHARGE_DATE  STRING
 )WITH(
    type ='kafka11',
    bootstrapServers ='slave2:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='ipc_diag_service_h',
    groupId='CDXT',
    parallelism ='1',
    timezone='Asia/Shanghai',
    sourcedatatype ='json'
 );

CREATE TABLE ipc_diag_service_d(
    TOTAL_AMT DECIMAL,
	DIAG_SERVICE_H_ID STRING
 )WITH(
    type ='kafka11',
    bootstrapServers ='slave2:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='ipc_diag_service_d',
    groupId='CDXT',
    parallelism ='1',
    timezone='Asia/Shanghai',
    sourcedatatype ='json'
 );

 -- 在院人数 使用 WATERMARK 时需要设置环境变量  "time.characteristic":"eventTime"
 --  默认是 PROCTIME  使用 WATERMARK 后该框架会设置 ROWTIME  2 选1
 CREATE TABLE ipi_registration(
	ID STRING,
	S_BRZTBH_DM STRING,
	HEALTH_SERVICE_ORG_ID STRING,
	REGISTRATION_DATE timestamp,
	WATERMARK FOR REGISTRATION_DATE AS withOffset( REGISTRATION_DATE , 200 )
 )WITH(
    type ='kafka11',
    bootstrapServers ='slave2:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='ipi_registration',
    groupId='CDXT',
    parallelism ='1',
    timezone='Asia/Shanghai',
    sourcedatatype ='json'
 );

--SINK
CREATE TABLE sink_result_1(
    PRO_NAME STRING,
    CODE STRING,
    LABEL STRING,
    DATA DECIMAL,
    DT STRING,
    REMARK STRING
 )WITH(
    type ='kafka10',
    bootstrapServers ='slave2:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='sink_result_1',
    groupId='CDXT',
    parallelism ='1',
    timezone='Asia/Shanghai',
    updateMode='upsert',
    sourcedatatype ='json'
 );

-- sink 费用
insert into sink_result_1
select
'big_screen' as PRO_NAME,
 'ipc_charge_fee' as CODE,
 '住院费用' as LABEL,
 sum(a.AMT) as DATA,
 PROC_DATE as DT,
 HEALTH_SERVICE_ORG_ID as REMARK
from(

SELECT
SUM(D.TOTAL_AMT) AMT,
DATE_FORMAT(H.PROCTIME, 'yyyy-MM-dd') as PROC_DATE,
H.HEALTH_SERVICE_ORG_ID
FROM ipc_drug_presc_h H
     INNER JOIN ipc_drug_presc_d D ON D.DRUG_PRESC_H_ID=H.ID
      GROUP BY H.HEALTH_SERVICE_ORG_ID ,DATE_FORMAT(H.PROCTIME, 'yyyy-MM-dd')
UNION ALL

SELECT
SUM(D.TOTAL_AMT) AMT,
DATE_FORMAT(H.PROCTIME, 'yyyy-MM-dd') as PROC_DATE,
H.HEALTH_SERVICE_ORG_ID
FROM ipc_diag_service_h H
     INNER JOIN ipc_diag_service_d D ON D.DIAG_SERVICE_H_ID=H.ID
	GROUP BY H.HEALTH_SERVICE_ORG_ID,DATE_FORMAT(H.PROCTIME, 'yyyy-MM-dd')

)a GROUP BY HEALTH_SERVICE_ORG_ID,PROC_DATE;
-- 住院Reich 窗口函数TUMBLE(ROWTIME, INTERVAL '1' DAY )
insert into sink_result_1
select
'big_screen' as PRO_NAME,
 'ipi_count' as CODE,
 '住院人次' as LABEL,
 count(1)  as DATA,
 DATE_FORMAT(ipi.ROWTIME, 'yyyy-MM-dd') as DT,
 HEALTH_SERVICE_ORG_ID as REMARK
from  ipi_registration ipi
where S_BRZTBH_DM in('20','30','65')
group by HEALTH_SERVICE_ORG_ID, DATE_FORMAT(ipi.ROWTIME, 'yyyy-MM-dd')