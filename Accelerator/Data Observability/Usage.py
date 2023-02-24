from Connection import *


class Usage:

    def __init__(self):
        self.connection = Connection()
        self.conn = self.connection.get_connection()

    def get_usage(self, table_name):
        usage_query = '''SELECT distinct SUT.schemaname ,SUT.seq_scan as 
        no_of_times_Accessed, tab.table_name, SAI.indexrelname ,       case when 
        SUT.seq_scan >0 then 'Used' else 'Not Used' end as Tables_Usability,       case when SAI.indexrelname  is 
        null then 'Index not used' else 'Index used' end as Index_Usability FROM Information_schema.tables Tab left 
        join pg_stat_user_tables SUT  on tab.table_name = SUT.relname left join pg_stat_all_indexes SAI on 
        tab.table_name = SAI.relname and SUT.schemaname = SAI.schemaname where SUT.schemaname = 'adaptiveai' and 
        table_name = ''' + "'" + table_name + "'"
        df = self.connection.extract_data(self.conn, usage_query)
        return df
