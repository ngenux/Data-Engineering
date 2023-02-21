from CalculateFreshness import *

if __name__ == '__main__':
    project_name = 'jaffle_shop'
    profiles_dir = 'C:/Users/ArvindYekkirala/.dbt'

    dbt = CalculateFreshness(project_name, profiles_dir)
    dbt_args = ['source', 'freshness']
    dbt.run_dbt(dbt_args)
