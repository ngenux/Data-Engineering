import pandas as pd
import numpy as np
from abc import ABC, abstractmethod


class Quality(ABC):

    @staticmethod
    def identify_datatypes(df):
        obj_cols = [col for col in df.columns if df[col].dtype.name == 'object']
        for col in obj_cols:
            try:
                df[col] = df[col].astype('float')
            except:
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    if df[col].nunique() < 20:
                        df[col] = df[col].astype('category')
        return df

    @staticmethod
    def classify_skewness(skew_val, threshold=0.5):
        if skew_val > threshold or skew_val < -threshold:
            return "highly skewed"
        elif skew_val > threshold / 2 or skew_val < -threshold / 2:
            return "moderately skewed"
        elif skew_val > -threshold / 2 and skew_val < threshold / 2:
            return "slightly skewed"
        else:
            return "approximately symmetric"

    @staticmethod
    def calculate_outlier_range(df,col):
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        return (lower_bound, upper_bound)

    @abstractmethod
    def return_dataframe_quality(self, df, cols):
        pass


class NumericalQuality(Quality):

    def return_dataframe_quality(self, df, cols):
        temp_num_list = []
        for col in cols:
            coldict = {}
            coldict['Name'] = col
            coldict['Count'] = df[col].count()
            coldict['Zeros'] = (df[col] == 0).sum()
            coldict['Negatives'] = (df[col] < 0).sum()
            coldict['Missing Values'] = df[col].isnull().sum()
            coldict['% Missing Values'] = np.round(100 * df[col].isnull().mean(), 2)
            coldict['Unique Values'] = df[col].nunique()
            coldict['Minimum'] = np.round(df[col].min(), 2)
            coldict['Maximum'] = np.round(df[col].max(), 2)
            coldict['Mean'] = np.round(df[col].mean(), 2)
            coldict['Median'] = np.round(df[col].median(), 2)
            coldict['Skewness'] = np.round(df[col].skew(), 2)
            coldict['Skewness_Category'] = Quality.classify_skewness(coldict['Skewness'], threshold=0.5)
            lower_bound, upper_bound = Quality.calculate_outlier_range(df, col)
            coldict['Outlier Range'] = f'({lower_bound}, {upper_bound})'
            num_df = pd.DataFrame(coldict.items()).T
            new_header = num_df.iloc[0]
            num_df = num_df[1:]
            num_df.columns = new_header
            temp_num_list.append(num_df)
        num_df = pd.concat(temp_num_list)
        return num_df


class CategoricalQuality(Quality):

    def return_dataframe_quality(self, df, cols):
        temp_cat_list = []
        for col in cols:
            coldict = {}
            coldict['Name'] = col
            coldict['Count'] = df[col].count()
            coldict['Zeros'] = (df[col] == 0).sum()
            coldict['Missing Values'] = df[col].isnull().sum()
            coldict['% Missing Values'] = np.round(100 * df[col].isnull().mean(), 2)
            coldict['Unique Values'] = df[col].nunique()
            coldict['Mode'] = df[col].mode().values[0]
            cat_df = pd.DataFrame(coldict.items()).T
            new_header = cat_df.iloc[0]
            cat_df = cat_df[1:]
            cat_df.columns = new_header
            temp_cat_list.append(cat_df)
        cat_df = pd.concat(temp_cat_list)
        return cat_df
