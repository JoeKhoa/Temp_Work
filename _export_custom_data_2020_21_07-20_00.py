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
                    AND pp.plan_sid IS NOT NULL
                    """
        query_employee ="""
                            AND e.sid = REPLACE('{employee}','-','')
                        """.format(employee = employee) if employee else """ """
        query_store =   """
                        AND pp.store_sid = '{store}'
                        """.format(store = store) if store else """ """
        sub_query_1 = """
                    SELECT pp.store_sid, pp.plan_sid
                    FROM plan_participaint pp
                    """+query_province+ " WHERE 1 " +query_where+query_store
        query_campaign = """
                WHERE p.campaign_sid = '{campaign}'
                """.format(campaign = campaign) if campaign else """ """            
        sub_query_2 = """
            SELECT p.sid
            FROM plan p
            """+query_campaign
        
        # very_start ="""(SELECT p.created_at
        #             FROM plan_form p
        #             ORDER BY created_at
        #             ASC
        #             LIMIT 1)            
        #             """
        # very_end =  """(SELECT p.created_at
        #             FROM plan_form p
        #             ORDER BY created_at
        #             DESC
        #             LIMIT 1)"""
        # no_day = from_date or to_date
        # from_date = from_date if from_date else very_start
        # to_date = to_date if to_date else very_end
        # query_date ="""
        #             AND p.start_date >= '{from_date}'
        #             AND p.start_date  <= '{to_date}'
        #             """.format(from_date=from_date, to_date=to_date) if no_day else """ """
        query_date =""" """

        import datetime
        from datetime import timedelta  
        if from_date:
            date_object = datetime.datetime.strptime(str(from_date), '%Y-%m-%d')    
            from_date= str(date_object)
        if to_date:
            date_object = datetime.datetime.strptime(str(to_date), '%Y-%m-%d')+timedelta(days=1)-timedelta(microseconds=1)
            to_date= str(date_object)
        # print( from_date, to_date)
        query_date_plf = """
                    AND plf.created_at >= '{from_date}'
                    AND plf.created_at <= '{to_date}'
        """.format(from_date=from_date, to_date=to_date) if from_date and to_date else """ """
        query = """              
                SELECT  tab_full_lv_1.*, s.name AS STORE
                FROM `store` AS s 
                INNER JOIN  ("""+sub_query_1+""") AS plp
                ON s.sid = plp.store_sid
                INNER JOIN  ("""+sub_query_2+""") AS p
                ON p.sid = plp.plan_sid 
                INNER JOIN plan_form AS plf
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
                                                    SELECT plf.sid as plan_form_sid
                                                    FROM plan_form AS plf 
                                                    INNER JOIN (
                                                        SELECT e.sid 
                                                        FROM employee AS e
                                                        WHERE NOT find_in_set(e.username,'tien.hiep,pix.pg,pixaccount,pixaccount2,accounttest,acctestv2,acctestv3,acctestv4')
                                                        """ + query_employee + """
                                                    ) AS e
                                                    ON plf.pg_sid = e.sid       
                                                    INNER JOIN (
                                                        SELECT  p.sid
                                                        FROM `plan` AS p     
                                                        WHERE 1 """ + query_date + """                                                              
                                                    ) p
                                                    ON p.sid = plf.plan_sid
                                                    WHERE 1 """ + query_date_plf + """                                                              
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
            workbook = writer.book
            # Get Sheet1
            cell_format = workbook.add_format()
            cell_format.set_font_size(15)
            cell_format.set_bold()
            worksheet.write(1, 1, "REPORT BY CUSTOMER",cell_format)
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

    export_plan_headers = [ 'STT', 'Khu vực', 'Ngày thực hiện',	'Mã số CH',	'CH', 'SDT', 'Tên PG',	'SL ca làm việc'] # add more product type

    @classmethod
    def export_plan(cls, from_date="", to_date="", province="", campaign="", store="", employee=""):
        """ status_plan = DONE for count work_shif / qc_status = DONE for qc only not for shift count"""
        query = """                                  
                SELECT p.*, fc.qc_answer_code, fc.status as form_qc_status, e.phone_number, e.full_name, sam.label, s.name as store_name, s.code as store_code, z.name as zone_name, fqa.label as form_qc_label
                FROM form_qc AS fc
                INNER JOIN (
                    SELECT  pf.sid as plan_form_sid , p.sid as plan_sid, pf.pg_sid, p.campaign_sid, p.status as plan_status
                    FROM `plan_form` AS pf
                    INNER JOIN (
                        SELECT p.sid, p.campaign_sid, p.status
                        FROM plan AS p
                        WHERE p.start_date > '2020-07-01'
                        AND p.campaign_sid = '8c8426b270b545d893eadbf5ab5bf24a'
                    )AS p
                    ON pf.plan_sid = p.sid
                ) AS p
                ON p.plan_form_sid = fc.form_sid
                INNER JOIN employee AS e
                ON p.pg_sid = e.sid
                INNER JOIN sampling AS sam
                ON p.campaign_sid =  sam.campaign_id
                INNER JOIN plan_participaint AS pp
                ON  p.plan_sid = pp.plan_sid
                INNER JOIN store AS s
                ON pp.store_sid = s.sid
                INNER JOIN zone AS z
                ON s.zone_id = z.id
                INNER JOIN form_qc_answer AS fqa
                ON  fc.qc_answer_code = fqa.code
                ORDER BY s.sid, e.sid
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
            dynamic_col_header = cls.get_product_type(data, 'label')  + cls.get_product_type(data, 'form_qc_label')
            df.columns = cls.export_plan_headers + dynamic_col_header    # semi-dynamic header            
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
        # header = 
        label_list =  cls.get_product_type(data, 'label')
        qc_label_list =  cls.get_product_type(data, 'form_qc_label')
        results = []
        copied_data = data.copy()
        store_list = cls.get_product_type(data, 'store_code')
        new_data = {}
        for i in store_list:
            new_data[i] = []
        for store in store_list: 
            for i,v in enumerate(copied_data):
                if v['store_code'] == store:
                    new_data[store].append(v)
                    # copied_data.pop(i)

        pg_list_in_store = new_data.copy()        
        """ build object(dict) like  hierachy:
            1.store
                   1.1.pg_name """
        for key, value in pg_list_in_store.items():
            pg_list_in_store[key] = {}

        for key, value in new_data.items():
            pg_sid_list = cls.get_product_type(value, 'pg_sid')
            for pg_sid in pg_sid_list:
                pg_list_in_store[key][pg_sid] = []


        for key, value in new_data.items():
            for k, v in pg_list_in_store[key].items():            
                for _k1, _v1 in enumerate(value):
                    if _v1['pg_sid'] == k:
                        pg_list_in_store[key][k].append(_v1)
        
        # for key, value in pg_list_in_store.items():
        #     print(key,'-', len(value))
        #     pass            
        #     for k, v in pg_list_in_store[key].items():
        #         print(k,'-', len(v))
        #         pass
        count_order = 0
        for key_store, value_store in pg_list_in_store.items():            
            for key_name, value_name in value_store.items():
                count_order += 1
                item = [
                        count_order,
                        value_name[0]['zone_name'],
                        '01-07-2020',
                        value_name[0]['store_code'],
                        value_name[0]['store_name'],
                        value_name[0]['phone_number'],
                        value_name[0]['full_name'],                        
                ]
                
                # add field SHIFT                
                count = 0
                for row in value_name: 
                    if row['form_qc_status'] == 'DONE':
                        count+=1
                item.append(int(count))

                # add shield product LABEL
                for lb_name in label_list:                
                    count = 0
                    for row in value_name: 
                        if row['label'] == lb_name and row['plan_status'] == 'DONE':
                            count+=1
                    item.append(count)                                    
                
                # add shield form QC ANSWER 
                for lb_name in qc_label_list:                
                    count = 0
                    for row in value_name: 
                        if row['form_qc_label'] == lb_name:
                            count+=1
                    item.append(count)                                

                results.append(item)
        return results

    @classmethod
    def get_product_type(cls, data, field_name):
        """ return product name exist in report -> to make dynamic header"""
        label_list = {''} # this a set NO-DUPLICATE value
        for field in data:
            label_list.add(str(field[field_name]))
        label_list = list(label_list)
        label_list.remove('')
        return label_list