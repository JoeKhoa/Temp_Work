import pandas as pd
from django.db import connection

from .helpers import dictfetchall
import logging


class ExportPlan(object):
    # pass
    export_plan_filename = 'Export_Plan_DONE'
    export_plan_file_ext = '.xlsx'
    export_plan_headers = [ 'STT', 'Tên Khách Hàng', 'Địa Chỉ', 'Phường/xã', 'Quận', 
                            'Tỉnh thành', 'Số  Điện Thoại', 'Mẫu 1-Bobby', 'Mẫu 2-Moony','Ngày dự sinh/ ngày sinh',
                            'Email', 'Bệnh Viện']


    @classmethod
    def export_plan(cls, from_date, to_date, province="", campaign="", store="", employee=""):

        query_province = """
                    INNER JOIN (SELECT s.sid
                    FROM store s
                    WHERE s.province_code = '{province}') AS str
                    ON str.sid = pp.store_sid
                    """.format(province=str(province)) if province else """ """
        query_where = """
                    WHERE 1
                    """
        query_employee ="""
                            AND pp.pg_sid = '{employee}'
                        """.format(employee = employee)\
            if employee else """ """
        query_store =   """
                        AND pp.store_sid = '{store}'
                        """.format(store = store)\
            if store else """ """
        # query_province = """
        #             INNER JOIN (SELECT s.sid
        #                 FROM store s
        #                 WHERE s.province_code = 'HCM') AS str
        #             ON str.sid = pp.store_sid
        #             """
        sub_query = """SELECT pp.store_sid, pp.plan_sid
                    FROM plan_participaint pp

                    """+query_province+query_where+query_store+query_employee
                    # +query_where
                    # +query_employee+query_store


        # sub_query = """
        #         SELECT pp.store_sid, pp.plan_sid
        #         FROM plan_participaint pp
        #         INNER JOIN (SELECT s.sid
        #                     FROM store s
        #                 WHERE s.province_code = 'HCM' )AS str
        #         ON str.sid = pp.store_sid
        #         WHERE 1
        # """

        query = """              
                SELECT  tab_full_lv_1.*, s.name AS STORE
                FROM `store` AS s 
                INNER JOIN  ("""+sub_query+""") AS plp
                ON s.sid = plp.store_sid
                INNER JOIN `plan` p 
                ON p.sid = plp.plan_sid
                INNER JOIN `plan_form` AS plf
                ON p.sid = plf.plan_sid
                INNER JOIN (
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
                                                        WHERE p.start_date >= '{from_date}'
                                                        AND p.start_date  < '{to_date}'
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


        """
        
        query = query.format(from_date=from_date, to_date=to_date)

# SELECT pp.store_sid, pp.plan_sid
# FROM plan_participaint pp
# INNER JOIN (SELECT s.sid
#            	FROM store s
#            WHERE s.province_code = 'HCM' )AS str
# ON str.sid = pp.store_sid
# WHERE 1
# AND pp.pg_sid = '445f5226844941afa8ecc5250eb8142f'
# AND pp.store_sid = '3e9fb48e64d346f7bcbbabb2304197b9'

        query = """{query}""".format(query=query)
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
        import re

        for idx, row in enumerate(data):
            results.append(
                (idx + 1,
                row['PARENT_NAME'],
                row['ADDRESS'],
                re.split(',',row['ADDRESS'])[-3],
                re.split(',',row['ADDRESS'])[-2],
                re.split(',',row['ADDRESS'])[-1],
                row['PHONE_NUMBER'],
                'x' if (row['SAMPLING_LABEL'] in ['Bobby','Bobby và Moony']) else '',
                'x' if (row['SAMPLING_LABEL'] in ['Moony','Bobby và Moony']) else '',
                row['BIRTHDAY_PREDICTION'],                 
                row['EMAIL'],
                row['STORE']
                )
            )
        return results
