

SELECT  tab_full_lv_1.*, s.name
FROM `store` AS s 
INNER JOIN `plan_participaint` AS plp
ON s.sid = plp.store_sid
INNER JOIN `plan` p 
ON p.sid = plp.plan_sid
INNER JOIN `plan_form` AS plf
ON p.sid = plf.plan_sid
INNER JOIN (
        SELECT big_tab.*, sam.label as SAMPLING_LABEL
                    FROM (                                                             select t1.plan_form_sid as Plan_FORM_SID, 
                                max(case when t2.`label` = 'Họ và tên bé' then t2.value end) FULL_NAME,
                                max(case when t2.`label` = 'Email' then t2.value end) EMAIL,
                                max(case when t2.`label` = 'Địa chỉ' then t2.value end) ADDRESS,
                                max(case when t2.`label` = 'Ngày sinh' then t2.value end) BIRTHDAY,
                                max(case when t2.`label` = 'Sampling' then t2.value end) SAMPLING,
                                max(case when t2.`label` = 'Ngày sinh dự kiến' then t2.value end) BIRTHDAY_PREDICTION,
                                max(case when t2.`label` = 'Số sổ' then t2.value end) MEDICAL_NUMBER,
                                max(case when t2.`label` = 'Số điện thoại' then t2.value end) PHONE_NUMBER,
                                max(case when t2.`label` = 'Tên cha/mẹ' then t2.value end) PARENT_NAME

                                from (
                                        SELECT DISTINCT pi.plan_form_sid
                                        FROM plan p
                                        INNER JOIN (
                                            SELECT DISTINCT pi.plan_sid, pi.plan_form_sid
                                            FROM plan_input as pi
                                            INNER JOIN (
                                                SELECT e.sid 
                                                FROM `employee` as e 
                                                WHERE NOT find_in_set(e.username, 'tien.hiep,pix.pg,pixaccount,pixaccount2,accounttest,acctestv2,acctestv3,acctestv4')
                                            ) AS rm_test_acc
                                            ON rm_test_acc.sid = pi.pg_sid
                                        ) as pi
                                        ON p.sid = pi.plan_sid
                                        WHERE p.start_date >= '2020-06-01'
                                        AND p.start_date  < '2020-07-01'
                                        AND p.status = 'DONE'
                    
                                ) t1
                            left join (
                                            SELECT  p.sid, data_.*
                                            FROM `plan` AS p
                                            INNER JOIN(
                                                SELECT it.label, data_row.*
                                                FROM `input_type` AS it
                                                INNER JOIN(
                                                        SELECT pi.value, pit.input_type_id, pi.plan_sid, pi.plan_form_sid
                                                        FROM `plan_input` AS pi
                                                        INNER JOIN plan_input_type AS pit
                                                        ON pi.input_type_id = pit.id
                                                        WHERE pi.pg_sid IS NOT NULL AND pi.plan_sid IS NOT NULL
                                                ) as data_row
                                                ON it.id = data_row.input_type_id
                                                ORDER BY data_row.plan_sid, data_row.input_type_id
                                            ) AS data_
                                            ON p.sid = data_.plan_sid
                                            ORDER BY  data_.plan_sid,  data_.input_type_id
                                        ) t2
                            on t1.plan_form_sid = t2.plan_form_sid
                            group by t1.plan_form_sid
                    ) big_tab
                    INNER JOIN sampling sam
                    ON sam.sid = REPLACE(big_tab.SAMPLING,'-','')
) tab_full_lv_1
ON tab_full_lv_1.Plan_FORM_SID = plf.sid


SELECT pi.value, pi.plan_form_sid  
FROM plan_input pi
WHERE pi.plan_form_sid  = 
(SELECT pi.plan_form_sid  
FROM `plan_input` pi 
WHERE `value` LIKE '0988433440')
-- WHERE  find_in_set(pi.value, '0988433440')



SELECT  tab_full_lv_1.*, s.name
FROM `store` AS s 
INNER JOIN `plan_participaint` AS plp
ON s.sid = plp.store_sid
INNER JOIN `plan` p 
ON p.sid = plp.plan_sid
INNER JOIN `plan_form` AS plf
ON p.sid = plf.plan_sid
INNER JOIN (
			SELECT pi.value, pi.plan_form_sid  
            FROM plan_input pi
            WHERE pi.plan_form_sid  = 
                    (SELECT pi.plan_form_sid  
                    FROM `plan_input` pi 
                    WHERE `value` LIKE '0988433440')
) tab_full_lv_1
ON tab_full_lv_1.plan_form_sid = plf.sid