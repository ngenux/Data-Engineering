import numpy as np
from evidently.report import Report
from evidently.metrics import DataDriftTable
from connections import *
from global_params import *


def identify_datatypes(df):
    obj_cols = [col for col in df.columns if df[col].dtype.name == 'object']
    for col in obj_cols:
        df1 = df.copy()
        # Extracting numerical features
        try:
            df[col] = df[col].astype('float')
        except:
            # Extracting datetime features
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                continue

        # Extracting features with type 'object'
    obj_cols = [col for col in df.columns if df[col].dtype.name == 'object']
    for col in obj_cols:
        # Extracting categorical features
        df1 = df.copy()
        if (df1[col].nunique() < 20):
            df[col] = df[col].astype('category')
    return df


def return_numerical_dataframe(df, num_cols):
    temp_num_list = []
    for col in num_cols:
        find_mode = False
        coldict = {}
        coldict['Name'] = col
        coldict['Datatype'] = df[col].dtypes.name
        # getting the type of numerical features
        if (df[col].nunique() < 20) and (df[col].isnull().mean() < 0.5):
            coldict['Type'] = 'Discrete'
            # separating boolean features
            if df[col].nunique() == 2:
                df1 = df.copy()
                df1[col] = df1[col].astype('bool')
                coldict['Datatype'] = df1[col].dtypes.name
            discrete_cols.append(col)
            find_mode = True
        elif (df[col].nunique() > 20) and (df[col].isnull().mean() < 0.5):
            coldict['Type'] = 'Continuous'
            continuous_cols.append(col)
        else:
            coldict['Type'] = 'Unable to identify.'

        coldict['Count'] = df[col].count()
        coldict['Zeros'] = (df[col] == 0).sum()
        coldict['Negatives'] = (df[col] < 0).sum()
        coldict['Missing Values'] = df[col].isnull().sum()
        coldict['% Missing Values'] = np.round(100 * df[col].isnull().mean(), 2)
        coldict['Unique Values'] = df[col].nunique()
        coldict['Minimum'] = np.round(df[col].min(), 2)
        coldict['Maximum'] = np.round(df[col].max(), 2)
        coldict['Mean'] = np.round(df[col].mean(), 2)
        coldict['Median'] = np.round(df[col].median(), 2)
        num_df = pd.DataFrame(coldict.items()).T
        new_header = num_df.iloc[0]
        num_df = num_df[1:]
        num_df.columns = new_header
        temp_num_list.append(num_df)
    num_df = pd.concat(temp_num_list)
    num_df.drop(['Type'], axis=1)
    return num_df


# Obtaining and storing the parameters of categorical features
def return_categorical_dataframe(df, cat_cols):
    temp_cat_list = []
    for col in cat_cols:
        coldict = {}
        coldict['Name'] = col
        coldict['Datatype'] = df[col].dtypes.name
        coldict['Count'] = df[col].count()
        coldict['Zeros'] = (df[col] == 0).sum()
        coldict['Missing Values'] = df[col].isnull().sum()
        coldict['% Missing Values'] = np.round(100 * df[col].isnull().mean(), 2)
        coldict['Unique Values'] = df[col].nunique()
        coldict['Mode'] = df[col].mode().values[0]
        cat_df = pd.DataFrame(coldict.items()).T
        new_header = cat_df.iloc[0]
        cat_df = cat_df[1:]
        cat_df.columns = new_header
        temp_cat_list.append(cat_df)
    cat_df = pd.concat(temp_cat_list)
    return cat_df


def return_date_dataframe(df, time_cols):
    for col in time_cols:
        coldict = {}
        coldict['Name'] = col
        coldict['Datatype'] = df[col].dtypes.name
        coldict['Count'] = df[col].count()
        coldict['Zeros'] = (df[col] == 0).sum()
        coldict['Missing Values'] = df[col].isnull().sum()
        coldict['% Missing Values'] = np.round(100 * df[col].isnull().mean(), 2)
        coldict['Unique Values'] = df[col].nunique()
        coldict['First timestamp'] = df[col].min()
        coldict['Latest timestamp'] = df[col].max()

        # calculating time step
        timestep_list = []
        for i in range(1, df.shape[0]):
            if df[col][i] != 'NaT' and df[col][i - 1] != 'NaT':
                timestep = df[col][i] - df[col][i - 1]
                timestep_list.append(timestep)

        # finding whether the data is uniform
        uniformity = False
        timestep_list = list(set(timestep_list))
        if len(timestep_list) == 1:
            uniformity = True
        coldict['Uniform data'] = uniformity
        if uniformity:
            coldict['Timestep'] = timestep_list[0]
        else:
            coldict['Timesteps'] = len(timestep_list)
    date_df = pd.DataFrame(coldict.items()).T
    new_header = date_df.iloc[0]  # grab the first row for the header
    date_df = date_df[1:]  # take the data less the header row
    date_df.columns = new_header  # set the header row as the df header
    return date_df


def calculate_data_drift(date_column, df):
    data_drift_dataset_report = Report(metrics=[
        DataDriftTable()
    ])
    prev_date = '2022-01-01'
    current_date = '2022-06-01'
    df_1 = df.loc[df[date_column] == prev_date]
    df_2 = df.loc[df[date_column] == current_date]
    data_drift_dataset_report.run(reference_data=df_1, current_data=df_2)
    final_df = pd.DataFrame(
        data_drift_dataset_report.as_dict()['metrics'][0]['result']['drift_by_columns']).reset_index()
    transposed_df = final_df.T
    new_header = transposed_df.iloc[0]
    transposed_df = transposed_df[1:]
    transposed_df.columns = new_header
    transposed_df.reset_index(inplace=True)
    transposed_df.drop('index', axis=1, inplace=True)
    return transposed_df
    # data_drift_dataset_report.save_html('sample_report.html')

def calculate_freshness(date_column, df):
    last_update_date = df[date_column].max()
    df = pd.DataFrame()
    df['column_name'] = pd.Series(date_column)
    df['last_updated_date'] = last_update_date
    return df

conn = get_connection()
df = return_tables_within_schema(conn, "adaptiveai")
list_of_tables = df['table_name'].unique().tolist()
list_of_tables = ['project_dim', 'client_dim', 'project_month_dim', 'client_month_dim']
list_of_tables = ['project_month_dim']

for table in list_of_tables:
    df = extract_data(conn, table)
    df = identify_datatypes(df)
    num_cols = [col for col in df if df[col].dtype.name in NUMERIC_TYPES]
    cat_cols = [col for col in df if df[col].dtype.name in CATEGORY_TYPES]
    time_cols = [col for col in df if df[col].dtype.name in DATETIME_TYPES]
    print(time_cols)
    if len(num_cols) > 0:
        num_df = return_numerical_dataframe(df, num_cols)
    if len(cat_cols) > 0:
        cat_df = return_categorical_dataframe(df, cat_cols)
    if len(time_cols) > 0:
        date_df = return_date_dataframe(df, time_cols)
        date_column = date_df['Name'].unique().tolist()[0]
        try:
            transposed_df = calculate_data_drift(date_column, df)
        except:
            transposed_df = pd.DataFrame()
            print("drift calculation skipped due to an error")
            pass
    last_update_df = calculate_freshness(date_column, df)
    with pd.ExcelWriter("output/" + table + ".xlsx") as writer:
        num_df.to_excel(writer, sheet_name="Numerical", index=False)
        cat_df.to_excel(writer, sheet_name="Categorical", index=False)
        transposed_df.to_excel(writer, sheet_name="Drift", index=False)
        last_update_df.to_excel(writer, sheet_name="Freshness", index=False)

# print(df.head(10))


# df = pd.read_csv('required_data.csv')
# df = identify_datatypes(df)
# num_cols = [col for col in df if df[col].dtype.name in NUMERIC_TYPES]
# cat_cols = [col for col in df if df[col].dtype.name in CATEGORY_TYPES]
# time_cols = [col for col in df if df[col].dtype.name in DATETIME_TYPES]
# if len(num_cols) > 0:
#    num_df = return_numerical_dataframe(df,num_cols)
#    dataframe_to_pdf(num_df, 'numerical.pdf')
# if len(cat_cols) > 0:
#    cat_df = return_categorical_dataframe(df,cat_cols)
#    dataframe_to_pdf(cat_df, 'categorical.pdf')
# if len(time_cols) > 0:
#    date_df = return_date_dataframe(df,time_cols)
#    date_column = date_df['Name'].unique().tolist()[0]
#    transposed_df = calculate_data_drift(date_column,df)
#    dataframe_to_pdf(transposed_df, 'drift.pdf')

# output = PdfFileWriter()

# try :
#    pdfOne = PdfFileReader(open("numerical.pdf", "rb"))
#    output.addPage(pdfOne.getPage(0))
# except :
#    print("Unable to identify any numeric")
# try :
#    pdfTwo = PdfFileReader(open("categorical.pdf", "rb"))
#    output.addPage(pdfTwo.getPage(0))
# except :
#    print("Unable to identify any categorical")
# try :
#    pdfThree = PdfFileReader(open("drift.pdf", "rb"))
#    output.addPage(pdfThree.getPage(0))
# except :
#    print("Dint find any date columns, so not calculating drift")


# outputStream = open(r"combined.pdf", "wb")
# output.write(outputStream)
# outputStream.close()


# path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
# config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
# pdfkit.from_file('sample_report.html', 'output.pdf', configuration = config,options = {
# 'no-stop-slow-scripts' : True,
# 'javascript-delay':'40000'
#       })

# HTML('sample_report.html').write_pdf('output.pdf')

# pdfkit.from_file('DataDrift.html', 'output.pdf', configuration = config, options = {
#    'javascript-delay':'10000'
#           })

# from PyPDF2 import PdfFileReader, PdfFileWriter
# from evidently.metrics import DatasetDriftMetric
# import pdfkit
# from scipy.stats import zscore
# from weasyprint import HTML
