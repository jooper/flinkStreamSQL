--oracle  mock数据


--门诊收入
insert into opr_registration_d
select * from opr_registration_d
where DISCOUNT_AFTER_AMT>13 and to_char(EXEC_DATE,'yyyy-mm-dd')='2019-12-10' and rownum<80
order by EXEC_DATE;
commit;


--手术例数
insert into mrm_first_page_operation
select * from mrm_first_page_operation where rownum<50;
commit;


--住院收入
insert into ipc_drug_presc_d
select * from ipc_drug_presc_d where rownum<25;
commit;

--住院收入
insert into ipc_diag_service_d
select * from ipc_diag_service_d where rownum<25;
commit;


--住院人次    算了distinct，所以插入同一批数据的时候页面不会变动
insert into ipi_registration
select * from ipi_registration where rownum<61;
commit;


--门诊人次   算了distinct，所以插入同一批数据的时候页面不会变动
insert into opc_registration
select * from opc_registration where rownum<89;
commit;


--会诊人次
insert into IPD_CONSULT_APPLY
select * from ipd_consult_apply
where rownum<25 and id is not null
order by req_date;
commit;

-- 医生治疗患者数，男女分布，全国患者分布,转诊人次
insert into mrm_first_page
select * from mrm_first_page where id in (
'fa68bbe84c4580815dcc',
'4907dc8431248081726e',
'49166ff932e08081726e',
'1e3ee7ab415480816c96',
'5177637c73838081726e',
'51970e9b7bb18081726e',
'51e2cb4a1c928081726e',
'530c078673288081726e',
'4dafba8934bf8081726e',
'5168250f709c8081726e',
'52ba1c7b598d8081726e',
'5184211f77098081726e',
'67418d803dec8081726e',
'776843220fc1892e2718',
'9104dd42029b808166de',
'd5282665589380815074',
'59800cef386b808166de',
'8ccb63571169808166de',
'f7bb05b558ce80812f54',
'ab32a4af3627808166de',
'92e515bd1739808166de',
'3173034670f9808166de',
'd43dd7684dfd808101fc',
'f2cae4ce563880812f54',
'6ebfdabb59c6808166de',
'cef989582a79808101fc',
'827490df3f5a808166de',
'2f3948b450f1808166de',
'f6edafed30fa80812f54',
'6ed1342a5a44808166de',
'92b9732811cc808166de'
) and rownum< 101;
commit;


--检验
insert into mtw_lab_h
select * from mtw_lab_h where rownum<20;
commit;

--检查
insert into mtw_exam_h
select * from mtw_exam_h where rownum<20;
commit;



