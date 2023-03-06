import pandas as pd
import numpy as np


class Drift:

    def calculate_drift(self, date_column, df, num_cols):
        date_list = sorted(df[date_column].unique())
        drift = []
        for i in range(1, len(date_list)):
            prev_date = date_list[i - 1]
            current_date = date_list[i]
            data = df.loc[df[date_column] == prev_date]
            data = data[num_cols]
            new_data = df.loc[df[date_column] == current_date]
            new_data = new_data[num_cols]
            if data.shape[0] == 0 or new_data.shape[0] == 0:
                message = f"No data available for {prev_date} or {current_date}"
                drift.append({'message': message})
            else:
                for column in data.columns:
                    mean_old = np.mean(data[column])
                    std_old = np.std(data[column])

                    mean_new = np.mean(new_data[column])
                    std_new = np.std(new_data[column])

                    mean_drift = np.abs(mean_old - mean_new)
                    std_drift = np.abs(std_old - std_new)

                    drift.append({'column': column, 'prev_date': prev_date, 'current_date': current_date,
                                  'mean_drift': mean_drift, 'std_drift': std_drift})

        return drift

    def get_drift_df(self, date_column, df, num_cols):
        drift = self.calculate_drift(date_column, df, num_cols)
        drift_df = pd.DataFrame(drift)
        return drift_df
