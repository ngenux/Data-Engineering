import subprocess
import pandas as pd
from Utils import *
import re


class VolumeCheck:
    def __init__(self, project_name):
        self.project_root = f'C:/Users/ArvindYekkirala/Documents/Git - DataEngineering/Data-Engineering/Upskill/DBT/{project_name}'
        self.utils = Utils()

    def run_dbt(self, dbt_args):
        test_args = ['test']
        test_cmd = ['dbt', '--no-version-check'] + test_args
        test_output = subprocess.run(test_cmd, capture_output=True, text=True, cwd=self.project_root)

        path = "C:/Users/ArvindYekkirala/Documents/Git - " \
               "DataEngineering/Data-Engineering/Upskill/DBT/jaffle_shop/output/volume.xlsx"
        sheet_name = "volume"

        error_lines = test_output.stdout.split('\n')
        volume_fail_list = []
        for line in error_lines:
            if 'FAIL' in line:
                df = pd.DataFrame()
                match = re.search(r'\b\w+_\w+\b', line)
                df['error'] = pd.Series("Volume test failed : " + match.group())
                volume_fail_list.append(df)
            else:
                continue
        df = pd.concat(volume_fail_list, axis=0)
        self.utils.save_excel_file(path, sheet_name, df)
