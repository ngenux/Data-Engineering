import pandas as pd


class Utils:

    def save_excel_file(self, path, sheet_name, df):
        with pd.ExcelWriter(path, engine="openpyxl", mode='a') as writer:
            workBook = writer.book
            try:
                workBook.remove(workBook[sheet_name])
            except:
                print("Worksheet does not exist")
            finally:
                df.to_excel(writer, sheet_name=sheet_name, index = False)
                writer.save()





