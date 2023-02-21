import subprocess
from Utils import *
import pandas as pd


class CalculateFreshness:
    def __init__(self, project_name, profiles_dir):
        self.project_root = f'C:/Users/ArvindYekkirala/Documents/Git - DataEngineering/Data-Engineering/Upskill/DBT/{project_name}'
        self.profiles_dir = profiles_dir
        self.utils = Utils()

    def run_dbt(self, dbt_args):
        dbt_cmd = ['dbt', '--no-version-check'] + dbt_args
        output = subprocess.run(dbt_cmd, capture_output=True, text=True, cwd=self.project_root)
        path = "C:/Users/ArvindYekkirala/Documents/Git - " \
               "DataEngineering/Data-Engineering/Upskill/DBT/jaffle_shop/output/freshness.xlsx"
        sheet_name = "freshness"

        error_lines = output.stdout.split('\n')
        stale_list = []
        for line in error_lines:
            if 'ERROR STALE' in line:
                table_name = line.split('freshness of ')[-1].split(' ')[0]
                df = pd.DataFrame()
                df['error'] = pd.Series(f'There were stale freshness errors in table {table_name}.')
                stale_list.append(df)
            else:
                continue
        df = pd.concat(stale_list, axis=0)
        self.utils.save_excel_file(path, sheet_name, df)

        # print('No stale freshness errors were found.')

# dbt.main.handle_and_check(dbt_args)
# print(output.stdout)

# check if there were any errors in the output
# if 'ERROR STALE' in output.stdout:
#    print('There were stale freshness errors.')
# else:
#    print('No stale freshness errors were found.')

# check if there were any errors in the outputif 'ERROR STALE' in output.stdout:


# dbt_args = ['run', '--models', 'jaffle_shop','--vars', '{"customers_freshness": "true"}', '--vars', '{"orders_freshness": "true"}']
# dbt_args = ['run', '--models', 'jaffle_shop']
