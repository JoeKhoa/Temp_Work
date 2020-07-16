import pandas as pd
from django.db import connection

from .helpers import dictfetchall
import logging


class ExportPlan(object):
    export_plan_filename = 'Export_Plan_DONE'
    export_plan_file_ext = '.xlsx'

    export_plan_headers = [ 'STT', 'Tên Khách Hàng', 'Địa Chỉ', 'Số  Điện Thoại','Ngày sinh',
                            'Email', 'Bệnh Viện'] # need to add more product type


    @classmethod
    def export_plan(cls, from_date="", to_date="", province="", campaign="", store="", employee=""):

        query_province = """
                    INNER JOIN (SELECT s.sid
                                FROM store s
                                WHERE s.province_code = '{province}') AS str
                    ON str.sid = pp.store_sid                    
                    """.format(province=province) if province else """ """
        query_where = """
                    WHERE pp.plan_sid IS NOT NULL
                    """
        query_employee ="""
                            WHERE fos.pg_sid= '{employee}'
                        """.format(employee = employee) if employee else """ """
        query_store =   """
                        AND pp.store_sid = '{store}'
                        """.format(store = store) if store else """ """
        sub_query_1 = """
                    SELECT pp.store_sid, pp.plan_sid
                    FROM plan_participaint pp
                    """+query_province+query_where+query_store
        query_campaign = """
                WHERE p.campaign_sid = '{campaign}'
                """.format(campaign = campaign) if campaign else """ """            
        sub_query_2 = """
            SELECT p.sid
            FROM plan p
            """+query_campaign
        
        very_start ="""(SELECT p.start_date
                    FROM plan p
                    ORDER BY start_date
                    ASC
                    LIMIT 1)            
                    """
        very_end =  """(SELECT p.end_date
                    FROM plan p
                    ORDER BY end_date
                    DESC
                    LIMIT 1)
                    """
        no_day = from_date or to_date
        from_date = from_date if from_date else very_start
        to_date = to_date if to_date else very_end
        query_date ="""
                    AND p.start_date >= '{from_date}'
                    AND p.end_date  < '{to_date}'
                    """.format(from_date=from_date, to_date=to_date) if no_day else """ """

        query = """              
                SELECT  tab_full_lv_1.*, s.name AS STORE
                FROM `store` AS s 
                INNER JOIN  ("""+sub_query_1+""") AS plp
                ON s.sid = plp.store_sid
                INNER JOIN  ("""+sub_query_2+""") AS p
                ON p.sid = plp.plan_sid
                INNER JOIN (
                    SELECT DISTINCT pf.plan_sid, pf.sid
                    FROM plan_form AS  pf
                    INNER JOIN (
                        SELECT fos.form_sid
                        FROM `form_sampling` AS fos"""+ query_employee +"""
                    ) AS fos
                    ON fos.form_sid = pf.sid    
                )AS plf
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
                                                    WHERE 1 """ + query_date + """                                                              
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

        query = """{query}""".format(query=query)
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = dictfetchall(cursor)
        return rows


    @classmethod
    def write_data_to_file(cls, data, file_path):
        try:
            df = pd.DataFrame(cls._parse_data_for_export_data(data))
            # df = pd.DataFrame(data) # use **dynamic data + dynamic-header
            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            df.columns = cls.export_plan_headers + cls.get_product_type(data)      # semi-dynamic header
            # df.columns = cls.export_plan_headers      # Hard code header
            df = df.astype(str)
            df.to_excel(writer, sheet_name=cls.export_plan_filename, startrow=5, startcol=0, index=False)
            worksheet = writer.sheets[cls.export_plan_filename]
            worksheet.write(1, 1, "REPORT BY CUSTOMER").set_bold()
            import datetime
            time_ = str(datetime.datetime.now()).split(".")[0]
            worksheet.write(2, 1, time_)
            writer.save()
            writer.close()
        except Exception as ex:
            return False, str(ex)
        return (True, None)

    @classmethod
    def _parse_data_for_export_data(cls, data):
        results = []
        label_list = cls.get_product_type(data)

        for idx, row in enumerate(data):
            item = [
                    idx + 1,
                    row['PARENT_NAME'],
                    row['ADDRESS'],
                    row['PHONE_NUMBER'] if row['PHONE_NUMBER'] else '',
                    row['BIRTHDAY_PREDICTION'] if row['BIRTHDAY_PREDICTION'] else '',
                    row['EMAIL'] if row['EMAIL'] else '',
                    row['STORE'] if row['STORE'] else ''
            ]
            for i in label_list:                
                if i == row['SAMPLING_LABEL'] :                
                    item.append('1')
                else:
                    item.append('')
            results.append(item)
        return results

    @classmethod
    def get_product_type(cls, data):
        """ return product name exist in report -> to make dynamic header"""
        label_list = {''} # this a set NO-DUPLICATE value
        for field in data:
            label_list.add(str(field['SAMPLING_LABEL']))
        label_list = list(label_list)
        label_list.remove('')
        return label_list


class ExportReportByDate(object):

    export_plan_filename = 'Export_Plan_DONE'
    export_plan_file_ext = '.xlsx'

    export_plan_headers = [ 'STT', 'Tên Khách Hàng', 'Địa Chỉ', 'Số  Điện Thoại','Ngày dự sinh/ ngày sinh',
                            'Email', 'Bệnh Viện'] # need to add more product type


    @classmethod
    def export_plan(cls, from_date="", to_date="", province="", campaign="", store="", employee=""):
    
        query = """                                  
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
                WHERE p.status = 'DONE'                                                                                        
        """
        query = """{query}""".format(query=query)
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = dictfetchall(cursor)
        return rows

    @classmethod
    def write_data_to_file(cls, data, file_path):
        try:
            # df = pd.DataFrame(cls._parse_data_for_export_data(data))
            df = pd.DataFrame(data) # use **dynamic data + dynamic-header
            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            # df.columns = cls.export_plan_headers + cls.get_product_type(data)      # semi-dynamic header
            # df.columns = cls.export_plan_headers      # Hard code header
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
        label_list = cls.get_product_type(data)

        for idx, row in enumerate(data):
            item = [
                    idx + 1,
                    row['PARENT_NAME'],
                    row['ADDRESS'],
                    row['PHONE_NUMBER'] if row['PHONE_NUMBER'] else '',
                    row['BIRTHDAY_PREDICTION'] if row['BIRTHDAY_PREDICTION'] else '',
                    row['EMAIL'] if row['EMAIL'] else '',
                    row['STORE'] if row['STORE'] else ''
            ]
            for i in label_list:                
                if i == row['SAMPLING_LABEL'] :                
                    item.append(i)
                else:
                    item.append('')
            results.append(item)
        return results

    @classmethod
    def get_product_type(cls, data):
        """ return product name exist in report -> to make dynamic header"""
        label_list = {''} # this a set NO-DUPLICATE value
        for field in data:
            label_list.add(str(field['SAMPLING_LABEL']))
        label_list = list(label_list)
        label_list.remove('')
        return label_list