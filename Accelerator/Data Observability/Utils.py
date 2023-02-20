import pandas as pd


class Utils:

    def save_excel_file(self, path, sheet_name, df):
        with pd.ExcelWriter(path,mode = "a", engine="openpyxl",if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
