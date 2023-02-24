import dbt.config
import subprocess

dbt.config.PROJECT_ROOT = 'C:/Users/ArvindYekkirala/Documents/Git - ' \
                                  'DataEngineering/Data-Engineering/Upskill/DBT/jaffle_shop'
dbt.config.PROFILES_DIR = 'C:/Users/ArvindYekkirala/.dbt'

dbt_args = ['source', 'freshness']
dbt_cmd = ['dbt', '--no-version-check'] + dbt_args

output = subprocess.run(dbt_cmd, capture_output=True, text=True)

error_lines = output.stdout.split('\n')
for line in error_lines:
    if 'ERROR STALE' in line:
        table_name = line.split('freshness of ')[-1].split(' ')[0]
        print(f'There were stale freshness errors in table {table_name}.')
    else:
        continue