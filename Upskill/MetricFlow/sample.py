import metricflow


from typing import List
import pandas as pd

def filter_by_Date(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:

    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    filtered_df = df.loc[mask]
    return filtered_df

def aggregate_by_Day(df: pd.DataFrame, metric_name: str) -> List[float]:

    df['date'] = pd.to_datetime(df['date'])
    df = df.groupby(['date']).sum()
    aggregated_data = df[metric_name].tolist()
    return aggregated_data




# Define a metric schema
schema = metricflow.MetricSchema(
    name='revenue',
    description='Total revenue of the company',
    data_type='float',
    units='USD',
    domain='financial',
    owner='finance',
)

# Create a metric instance
instance = metricflow.MetricInstance(
    schema=schema,
    source='database',
    time_range=('2022-01-01', '2022-01-31'),
)

# Define a data pipeline
pipeline = metricflow.DataPipeline(
    source='database',
    steps=[
        metricflow.Step(
            name='filter_by_date',
            func=filter_by_date,
            args=('2022-01-01', '2022-01-31'),
        ),
        metricflow.Step(
            name='aggregate_by_day',
            func=aggregate_by_day,
        ),
    ],
)

# Run the data pipeline and save the results to the metric instance
result = pipeline.run()
instance.save(result)


