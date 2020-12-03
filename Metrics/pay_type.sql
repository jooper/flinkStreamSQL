--支付方式
-- select d.discount_after_amt 金额,
--        fs.s_zffs_cmc 支付方式
-- from opr_registration_d d
--      inner join opr_pay opr_pay on opr_pay.registration_d_id=d.id
--      inner join pub_zffs fs on fs.s_zffs_dm=opr_pay.s_zffs_dm
-- 	inner join opr_registration opr on opr.id=d.opr_registration_id
-- where d.exec_date>=trunc(sysdate)
--  ${if(len(health_service_org_id)==0,""," and opr.health_service_org_id in ("+health_service_org_id+") ")}
-- union all
-- select d.total_amt,
--        fs.s_zffs_cmc 支付方式
-- from opc_drug_presc_h_charge h
--      inner join opc_drug_presc_d_charge d on d.drug_presc_h_charge_id=h.id
--      inner join opc_drug_presc_pay drugp on drugp.drug_presc_d_charge_id=d.id
--      inner join pub_zffs fs on fs.s_zffs_dm=drugp.s_zffs_dm
-- where h.charge_date>=trunc(sysdate)
--  ${if(len(health_service_org_id)==0,""," and h.health_service_org_id in ("+health_service_org_id+") ")}
-- union all
-- select d.total_amt,
--       fs.s_zffs_cmc 支付方式
-- from opc_diag_service_h_charge h
--      inner join opc_diag_service_d_charge d on d.diag_service_h_charge_id=h.id
--      inner join opc_diag_service_pay diagp on diagp.diag_service_d_charge_id=d.id
--      inner join pub_zffs fs on fs.s_zffs_dm=diagp.s_zffs_dm
-- where h.charge_date>=trunc(sysdate)
--  ${if(len(health_service_org_id)==0,""," and h.health_service_org_id in ("+health_service_org_id+") ")}




opr_registration_d
opr_pay
pub_zffs
opr_registration


opc_drug_presc_h_charge
opc_drug_presc_d_charge
opc_drug_presc_pay
pub_zffs

opc_diag_service_h_charge
opc_diag_service_d_charge
opc_diag_service_pay
pub_zffs