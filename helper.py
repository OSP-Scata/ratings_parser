import pandas as pd

excel_file = 'workfiles/Внутр_Рейтинг клиник.xlsx'
xl = pd.ExcelFile(excel_file)
sheets = xl.sheet_names


def prepare_dataframe(file, sheet, field1, field2, field3, field4, field5=None, field6=None):
    print('Парсим', sheet)
    df = pd.read_excel(file, sheet_name=sheet)
    df.rename(columns={field1: field2}, inplace=True)
    df.rename(columns={field3: field4}, inplace=True)
    if field5 is not None and field6 is not None:
        df.rename(columns={field5: field6}, inplace=True)
        df = df[[field2, field4, field6]]
    else:
        df = df[[field2, field4]]

    df.drop(df.tail(4).index, inplace=True)
    df[field4] = df[field4].str.strip()
    if sheet != sheets[3]:
        df.dropna(inplace=True)
    return df


def files_checking(filename):
    try:
        df = pd.read_csv(filename)
        return df
    except FileNotFoundError:
        return None
