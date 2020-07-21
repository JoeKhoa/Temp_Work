Table 1: t1
ID    Name
1     Jim
2     Bob
3     John

Table 2: t2
ID    key           value
1     address       "X Street"
1     city          "NY"
1     region        "NY"
1     country       "USA"
1     postal_code   ""
1     phone         "123456789"

The desired result right from the MySQL query is:
ID    Name    address    city    region   country   postal_code   phone
1     Jim     X Street   NY      NY       USA       NULL          123456789
2     Bob     NULL       NULL    NULL     NULL      NULL          NULL
3     John    NULL       NULL    NULL     NULL      NULL          NULL

select t1.id,
  t1.name,
  max(case when t2.`key` = 'address' then t2.value end) address,
  max(case when t2.`key` = 'city' then t2.value end) city,
  max(case when t2.`key` = 'region' then t2.value end) region,
  max(case when t2.`key` = 'country' then t2.value end) country,
  max(case when t2.`key` = 'postal_code' then t2.value end) postal_code,
  max(case when t2.`key` = 'phone' then t2.value end) phone
from table1 t1
left join table2 t2
  on t1.id = t2.id
group by t1.id, t1.name



************************ MAIN CONCISE QUERRY *********************
select t1.plan_form_sid as Plan_FORM_SID, 
max(case when t2.`label` = 'Họ và tên bé' then t2.value end) FULL_NAME,
max(case when t2.`label` = 'Email' then t2.value end) EMAIL,
max(case when t2.`label` = 'Địa chỉ' then t2.value end) ADDRESS,
max(case when t2.`label` = 'Ngày sinh' then t2.value end) BIRTHDAY,
max(case when t2.`label` = 'Sampling' then t2.value end) SAMPLING,
max(case when t2.`label` = 'Ngày sinh dự kiến' then t2.value end) BIRTHDAY_PREDICTION,
max(case when t2.`label` = 'Số sổ' then t2.value end) MEDICAL_NUMBER,
max(case when t2.`label` = 'Số điện thoại' then t2.value end) PHONE_NUMBER,
max(case when t2.`label` = 'Tên người nhận' then t2.value end) PARENT_NAME

from (
        SELECT DISTINCT pi.plan_form_sid
        FROM plan p
        INNER JOIN plan_input pi
        ON p.sid = pi.plan_sid
        INNER JOIN (
            SELECT e.sid
            FROM employee AS e
            WHERE e.sid = REPLACE('2d1edd11-1c17-444e-9f30-859cebc65ea8','-','')
        ) AS e
        ON pi.pg_sid = e.sid
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


************************** Very start _ main query *********************

select t1.plan_form_sid as Plan_FORM_SID, 
max(case when t2.`label` = 'Họ và tên bé' then t2.value end) FULL_NAME,
max(case when t2.`label` = 'Email' then t2.value end) EMAIL,
max(case when t2.`label` = 'Địa chỉ' then t2.value end) ADDRESS,
max(case when t2.`label` = 'Ngày sinh' then t2.value end) BIRTHDAY,
max(case when t2.`label` = 'Sampling' then t2.value end) SAMPLING,
max(case when t2.`label` = 'Ngày sinh dự kiến' then t2.value end) BIRTHDAY_PREDICTION,
max(case when t2.`label` = 'Số sổ' then t2.value end) MEDICAL_NUMBER,
max(case when t2.`label` = 'Số điện thoại' then t2.value end) PHONE_NUMBER,
max(case when t2.`label` = 'Tên người nhận' then t2.value end) PARENT_NAME

from (
        SELECT DISTINCT pi.plan_form_sid
        FROM plan p
        INNER JOIN (
            SELECT DISTINCT pi.plan_sid, pi.plan_form_sid
            FROM plan_input as pi
            INNER JOIN (
                SELECT e.sid 
                FROM `employee` as e 
                WHERE NOT find_in_set(e.username,'tien.hiep,pix.pg,pixaccount,pixaccount2,accounttest,acctestv2,acctestv3,acctestv4')
            ) AS rm_test_acc
            ON rm_test_acc.sid = pi.pg_sid
        ) as pi
        ON p.sid = pi.plan_sid
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



__________________________ GOOD SQL 157 __________________________

select t1.plan_form_sid as Plan_FORM_SID, 
max(case when t2.`label` = 'Họ và tên bé' then t2.value end) FULL_NAME,
max(case when t2.`label` = 'Email' then t2.value end) EMAIL,
max(case when t2.`label` = 'Địa chỉ' then t2.value end) ADDRESS,
max(case when t2.`label` = 'Ngày sinh' then t2.value end) BIRTHDAY,
max(case when t2.`label` = 'Sampling' then t2.value end) SAMPLING,
max(case when t2.`label` = 'Ngày sinh dự kiến' then t2.value end) BIRTHDAY_PREDICTION,
max(case when t2.`label` = 'Số sổ' then t2.value end) MEDICAL_NUMBER,
max(case when t2.`label` = 'Số điện thoại' then t2.value end) PHONE_NUMBER,
max(case when t2.`label` = 'Tên người nhận' then t2.value end) PARENT_NAME

from (
        SELECT DISTINCT plf.sid as plan_form_sid
        FROM plan_form AS plf
        INNER JOIN (
            SELECT e.sid 
            FROM employee AS e
            WHERE e.sid = REPLACE('2d1edd11-1c17-444e-9f30-859cebc65ea8','-','')
        ) AS e
        ON plf.pg_sid = e.sid
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