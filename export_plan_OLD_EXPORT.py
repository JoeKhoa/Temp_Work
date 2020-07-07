import pandas as pd
from django.db import connection

from .helpers import dictfetchall
import logging


class ExportPlan(object):
    export_plan_filename = 'Export_Plan_DONE'
    export_plan_file_ext = '.xlsx'
    # export_plan_headers = ['STT', 'Region', 'Store code', 'Store name',
    #                        'Plan code', 'Date start', 'Date end', 'Mer user', 'Mer name']
    # export_plan_headers = ['STT', 'Tên Khách Hàng', 'Địa Chỉ', 'Số  Điện Thoại',
    #                        'Ngày dự sinh/ ngày sinh', 'Mẫu']
    """ for  BEFORE JULY """
    export_plan_headers = [ 'STT', 'Tên Khách Hàng', 'Địa Chỉ', 'Phường/xã', 'Quận', 
                            'Tỉnh thành', 'Số  Điện Thoại', 'Mẫu 1', 'Mẫu 2','Ngày dự sinh/ ngày sinh',
                            'Email']


    @classmethod
    def export_plan(cls, from_date, to_date, plan_status, region_code=None):
        # query = """
        #     SELECT p.sid AS plan_sid, p.code AS plan_code, COALESCE(p.name,'N/A') As plan_name,
        #         p.start_date, p.end_date,
        #         pp.region_code, pp.region_name, pp.store_code,
        #         COALESCE(pp.store_name, 'N/A') as store_name, pp.username, COALESCE(pp.full_name,'N/A') as full_name
        #     FROM plan AS p
        #     LEFT JOIN (
        #         SELECT pp.plan_sid, pp.store_sid, pp.pg_sid,
        #             r.code as region_code, r.label as region_name,
        #             s.code as  store_code, s.name as store_name,
        #             e.username, e.full_name
        #         FROM plan_participaint as pp
        #         LEFT JOIN employee AS e
        #         ON e.sid = pp.pg_sid
        #         LEFT JOIN store AS s
        #         ON s.sid = pp.store_sid
        #         LEFT JOIN region as r
        #         ON r.id  = s.region_id
        #     ) AS pp
        #     ON p.sid = pp.plan_sid
        #     WHERE p.updated_at >= '{from_date}'
        #     AND p.updated_at  < '{to_date}'
        #     AND p.status = '{plan_status}'
        # """.format(from_date=from_date, to_date=to_date, plan_status=plan_status)
        query = """
              
                SELECT big_tab.*, sam.label as SAMPLING_LABEL
                FROM ( select t1.plan_form_sid as Plan_FORM_SID,
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
									SELECT pi.plan_form_sid
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
                                    # WHERE p.updated_at >= '{from_date}'
                                    # AND p.updated_at  < '{to_date}'
                                    # AND p.status = '{plan_status}'
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
        """.format(from_date=from_date, to_date=to_date, plan_status=plan_status)

# ********** tab_1:  plan_form_sid

									# SELECT pi.plan_form_sid
                                    # FROM plan p
                                    # INNER JOIN (
                                    #     SELECT DISTINCT pi.plan_sid, pi.plan_form_sid
                                    #     FROM plan_input pi
                                    # ) as pi
                                    # ON p.sid = pi.plan_sid

# ********** tab_2:  plan_form_sid


										# SELECT  p.sid, data_.*
                                        # FROM `plan` AS p
                                        # RIGHT JOIN(
                                        #     SELECT it.label, data_row.*
                                        #     FROM `input_type` AS it
                                        #     INNER JOIN(
                                        #             SELECT pi.value, pit.input_type_id, pi.plan_sid, pi.plan_form_sid
                                        #             FROM `plan_input` AS pi
                                        #             INNER JOIN plan_input_type AS pit
                                        #             ON pi.input_type_id = pit.id
                                        #     ) as data_row
                                        #     ON it.id = data_row.input_type_id
                                        #     ORDER BY data_row.plan_sid, data_row.input_type_id
                                        # ) AS data_
                                        # ON p.sid = data_.plan_sid
                                        # ORDER BY  data_.plan_sid,  data_.input_type_id


# **********

    #    SELECT big_tab.*, sam.label as SAMPLING_LABEL
    #             FROM ( select t1.sid as Plan_SID,
    #                         max(case when t2.`label` = 'Họ và tên bé' then t2.value end) FULL_NAME,
    #                         max(case when t2.`label` = 'Email' then t2.value end) EMAIL,
    #                         max(case when t2.`label` = 'Địa chỉ' then t2.value end) ADDRESS,
    #                         max(case when t2.`label` = 'Ngày sinh' then t2.value end) BIRTHDAY,
    #                         max(case when t2.`label` = 'Sampling' then t2.value end) SAMPLING,
    #                         max(case when t2.`label` = 'Ngày sinh dự kiến' then t2.value end) BIRTHDAY_PREDICTION,
    #                         max(case when t2.`label` = 'Số sổ' then t2.value end) MEDICAL_NUMBER,
    #                         max(case when t2.`label` = 'Số điện thoại' then t2.value end) PHONE_NUMBER,
    #                         max(case when t2.`label` = 'Tên cha/mẹ' then t2.value end) PARENT_NAME
    #                         from (
    #                                 SELECT p.sid 
    #                                 FROM plan p
    #                                 INNER JOIN (
    #                                 SELECT DISTINCT pi.plan_sid
    #                                 FROM plan_input pi
    #                                 ) as pi
    #                                 ON p.sid = pi.plan_sid
    #                         ) t1
    #                     left join (
    #                                     SELECT  p.sid, data_.*
    #                                     FROM `plan` AS p
    #                                     RIGHT JOIN(
    #                                         SELECT it.label, data_row.*
    #                                         FROM `input_type` AS it
    #                                         INNER JOIN(
    #                                                 SELECT pi.value, pit.input_type_id, pi.plan_sid
    #                                                 FROM `plan_input` AS pi
    #                                                 INNER JOIN plan_input_type AS pit
    #                                                 ON pi.input_type_id = pit.id
    #                                         ) as data_row
    #                                         ON it.id = data_row.input_type_id
    #                                         ORDER BY data_row.plan_sid, data_row.input_type_id
    #                                     ) AS data_
    #                                     ON p.sid = data_.plan_sid
    #                                     ORDER BY  data_.plan_sid,  data_.input_type_id
    #                                 ) t2
    #                     on t1.sid = t2.plan_sid
    #                     group by t1.sid
    #             ) big_tab
    #             INNER JOIN sampling sam
    #             ON sam.sid = REPLACE(big_tab.SAMPLING,'-','')





# SELECT e.username, e.sid 
# from `employee` e 
# WHERE NOT find_in_set(e.username, 'tien.hiep,pix.pg,pixaccount,pixaccount2,accounttest,acctestv2,acctestv3,acctestv4')



        if region_code:
            query = """
                {query}
                AND pp.region_code = '{region_code}'
            """.format(query=query, region_code=region_code)
        rows = []
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = dictfetchall(cursor)
        return rows

    @classmethod
    def write_data_to_file(cls, data, file_path):
        try:
            df = pd.DataFrame(cls._parse_data_for_export_data(data))
            # df = pd.DataFrame(data) # use **dynamic data + header
            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            df.columns = cls.export_plan_headers      # Hard code header
            df = df.astype(str)
            df.to_excel(writer, sheet_name=cls.export_plan_filename, startrow=5, startcol=0, index=False)
            writer.save()
            writer.close()
        except Exception as ex:
            return False, str(ex)
        return (True, None)

    @classmethod
    def _parse_data_for_export_data(cls, data):
        results = []

        for idx, row in enumerate(data):
            # results.append(
            #     (idx + 1,
            #      row['region_name'],
            #      row['store_code'],
            #      row['store_name'],
            #      row['plan_code'],
            #      row['start_date'],
            #      row['end_date'],
            #      row['username'],
            #      row['full_name'])
            # )
            results.append(
                (idx + 1,
                row['PARENT_NAME'],
                row['ADDRESS'],
                row['ADDRESS'],
                row['ADDRESS'],
                row['ADDRESS'],
                row['PHONE_NUMBER'],
                'x' if (row['SAMPLING_LABEL'] in ['Bobby','Bobby và Moony']) else '',
                'x' if (row['SAMPLING_LABEL'] in ['Moony','Bobby và Moony']) else '',
                row['BIRTHDAY_PREDICTION'],                 
                row['EMAIL'])
            )
        return results

# Stt	Họ tên đệm	Tên	Địa chỉ	Phường/xã	Quận	Tỉnh thành	Số điện thoại	Mẫu 1	Mẫu 2	Ngày dự sinh/ ngày sinh	Email
