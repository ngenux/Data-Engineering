from CalculateFreshness import *
from VolumeCheck import *

if __name__ == '__main__':
    project_name = 'jaffle_shop'
    profiles_dir = 'C:/Users/ArvindYekkirala/.dbt'

    dbt_freshness = CalculateFreshness(project_name, profiles_dir)
    dbt_freshness_args = ['source', 'freshness']
    dbt_freshness.run_dbt(dbt_freshness_args)

    dbt_volume = VolumeCheck(project_name)
    dbt_volume_args = ['run']
    dbt_volume.run_dbt(dbt_volume_args)

    # Generate docs
    docs_generate_cmd = ['dbt', 'docs', 'generate', '--no-version-check']
    subprocess.run(docs_generate_cmd,
                   cwd=f'C:/Users/ArvindYekkirala/Documents/Git - DataEngineering/Data-Engineering/Upskill/DBT/{project_name}')

    # Serve docs
    docs_serve_cmd = ['dbt', 'docs', 'serve', '--port', '9001']
    subprocess.run(docs_serve_cmd,
                   cwd=f'C:/Users/ArvindYekkirala/Documents/Git - DataEngineering/Data-Engineering/Upskill/DBT/{project_name}')
