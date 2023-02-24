import dbt.config
import subprocess

dbt.config.PROJECT_ROOT = 'C:/Users/ArvindYekkirala/Documents/Git - ' \
                                  'DataEngineering/Data-Engineering/Upskill/DBT/jaffle_shop'
dbt.config.PROFILES_DIR = 'C:/Users/ArvindYekkirala/.dbt'

# Run the 'dbt source freshness' command
freshness_args = ['source', 'freshness']
freshness_cmd = ['dbt', '--no-version-check'] + freshness_args
freshness_output = subprocess.run(freshness_cmd, capture_output=True, text=True)

# Print any freshness errors
error_lines = freshness_output.stdout.split('\n')
for line in error_lines:
    if 'ERROR STALE' in line:
        table_name = line.split('freshness of ')[-1].split(' ')[0]
        print(f'There were stale freshness errors in table {table_name}.')
    else:
        continue

test_args = ['test']
test_cmd = ['dbt', '--no-version-check'] + test_args
test_output = subprocess.run(test_cmd, capture_output=True, text=True)

error_lines = test_output.stdout.split('\n')
for line in error_lines:
    if 'FAIL' in line:
        print(f"Volume test failed :  {line}.")
    else:
        continue


