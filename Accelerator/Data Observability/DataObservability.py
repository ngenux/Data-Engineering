from Connection import *
from GlobalParams import GlobalParams
from DataObservabilityBackup import *
from Drift import *
from Quality import *
from Utils import *


class DataObservability:

    def __init__(self):
        self.connection = Connection()
        self.nq = NumericalQuality()
        self.cq = CategoricalQuality()
        self.do = DataObservabilityBackup()
        self.drift = Drift()
        self.schema_name = "adaptiveai"
        self.information_schema_query = """select table_name from information_schema.tables where table_schema = '""" + self.schema_name + """' 
                and table_name like '%dim%'"""
        self.utils = Utils()

    def generate_numercial_dataframe(self,df,num_cols,path):
        num_df = self.nq.return_dataframe_quality(df, num_cols)
        sheet_name = "Numerical"
        self.utils.save_excel_file(path, sheet_name, num_df)

    def generate_categorical_dataframe(self,df,cat_cols,path):
        cat_df = self.cq.return_dataframe_quality(df, cat_cols)
        sheet_name = "Categorical"
        self.utils.save_excel_file(path, sheet_name, cat_df)

    def generate_drift_dataframe(self,df, time_cols, num_cols,path):
        date_column = time_cols[0]
        try:
            drift_df = self.drift.get_drift_df(date_column, df, num_cols)
        except Exception as e:
            print(e)
            drift_df = pd.DataFrame()
            print("drift calculation skipped due to an error")
            pass
        sheet_name = "Drift"
        self.utils.save_excel_file(path, sheet_name, drift_df)

    def run_code(self):

        conn = self.connection.get_connection()
        table_names_df = self.connection.extract_data(conn, self.information_schema_query)
        # list_of_tables = table_names_df['table_name'].unique().tolist()
        list_of_tables = ['project_dim', 'client_dim', 'project_month_dim', 'client_month_dim']

        for table in list_of_tables:
            query = "select * from " + self.schema_name + "." + table
            path = "output/" + table + ".xlsx"
            df = self.connection.extract_data(conn, query)
            df = Quality.identify_datatypes(df)
            num_cols = [col for col in df if df[col].dtype.name in GlobalParams.NUMERIC_TYPES.value]
            cat_cols = [col for col in df if df[col].dtype.name in GlobalParams.CATEGORY_TYPES.value]
            time_cols = [col for col in df if df[col].dtype.name in GlobalParams.DATETIME_TYPES.value]
            if len(num_cols) > 0:
                self.generate_numercial_dataframe(df,num_cols,path)
            if len(cat_cols) > 0:
                self.generate_categorical_dataframe(df, cat_cols, path)
            if len(time_cols) > 0:
                self.generate_drift_dataframe(df, time_cols, num_cols,path)


