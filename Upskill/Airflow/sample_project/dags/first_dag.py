try :
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd


    print("Working")
except:
    print("Error")

def first_function_execute(**context):
    print("First function execute")
    context['ti'].xcom_push(key='mykey',value = "First function says hello")

def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key='mykey')
    data = [{"name":"arvind"}]
    df = pd.DataFrame(data)
    print(df.shape[0])
    print("This is second function, got this value from first " + instance)



#def first_function_execute(*args, **kwargs):
#    variable = kwargs.get("name","No Key")
#    print("Hello World" + variable)
#    return "Hello World" + variable

#def second_function_execute(*args, **kwargs):
#    variable = kwargs.get("name","No Key")
#    print("Hello World" + variable)
#    return "Hello World" + variable

with DAG(
    dag_id = "first_dag",
    schedule_interval = "@daily",
    default_args = {
        "owner" : "airflow",
        "retries" : 1,
        "retry_delay" : timedelta(minutes = 5),
        "start_date" : datetime(2021,1,1)
    },
    catchup = False
) as f:
    first_function_execute = PythonOperator(
        task_id = "first_function_execute",
        python_callable = first_function_execute,
        provide_context=True,
    op_kwargs = {"name" : "Arvind"}
        )

    second_function_execute = PythonOperator(
        task_id = "second_function_execute",
        python_callable = second_function_execute,
        provide_context=True
    #op_kwargs = {"name" : "Arvind"}
    )

first_function_execute >> second_function_execute

