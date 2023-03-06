import pandas as pd
import numpy as np


class DriftBackup:

    def calculate_drift(self, date_column, df, num_cols):
        prev_date = '2022-01-01'
        current_date = '2022-06-01'
        data = df.loc[df[date_column] == prev_date]
        data = data[num_cols]
        new_data = df.loc[df[date_column] == current_date]
        new_data = new_data[num_cols]
        drift = []
        if data.shape[0] == 0 or new_data.shape[0] == 0:
            message = "Either date column not available/ or no data for the given date"
            drift.append({'message': message})
            return drift
        else:
            for column in data.columns:
                mean_old = np.mean(data[column])
                std_old = np.std(data[column])

                mean_new = np.mean(new_data[column])
                std_new = np.std(new_data[column])

                mean_drift = np.abs(mean_old - mean_new)
                std_drift = np.abs(std_old - std_new)

                drift.append({'column': column, 'mean_drift': mean_drift, 'std_drift': std_drift})

            return drift

    def get_drift_df(self, date_column, df, num_cols):
        drift = self.calculate_drift(date_column, df, num_cols)
        drift_df = pd.DataFrame(drift)
        return drift_df
