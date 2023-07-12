import pandas as pd
from openpyxl import load_workbook

query = pd.read_csv(r'C:\Users\danil\Documents\Work\query.csv')
dohod = pd.read_excel(r'C:\Users\danil\Documents\Work\dohod.xlsx',1)
ops_type = pd.read_csv(r'C:\Users\danil\Documents\Work\ops_type.csv')

query_data_2020 = query.loc[query['__time'] == "Wed Jan 01 2020 03:00:00 GMT+0300 (Москва, стандартное время)"]
query_data_2021 = query.loc[query['__time'] == "Fri Jan 01 2021 03:00:00 GMT+0300 (Москва, стандартное время)"]

ops_type_data = ops_type[['index_ops', 'ops_type']]

dohod_alldata = dohod.loc[dohod['Группа должностей'] == "Оператор"]
dohod_data = dohod_alldata[['Индекс подразделения','Тип ОПС','Количество ставок по ШР']]

final_data = pd.DataFrame({'year' : [], 'value_type' : [], 'value' : [], 'ops' : [], 'ops_type' : []})

for index, row in query_data_2020.iterrows():

    druid = row['operator'] + row['operator_2']
    ops_num = row['organizationUnit']
    
    ops_type_row = ops_type_data.loc[ops_type_data['index_ops'] == ops_num]
    d_ops_type = ops_type_row.iloc[0]['ops_type']

    dohod_row = dohod_data.loc[dohod_data['Индекс подразделения'] == ops_num]
    if dohod_row.empty == True:
        d_value = 'None'
    else :
        d_value = dohod_row.iloc[0]['Количество ставок по ШР']

    y2020 = pd.Series(["Wed Jan 01 2020 03:00:00 GMT+0300 (Москва, стандартное время)", "Wed Jan 01 2020 03:00:00 GMT+0300 (Москва, стандартное время)", "Wed Jan 01 2020 03:00:00 GMT+0300 (Москва, стандартное время)"], name= 'year')
    value_type = pd.Series(["druid", "расчетная", "фактическая"], name = 'value_type')
    value = pd.Series([druid, row['employee_count'], d_value], name = 'value')
    ops = pd.Series([ops_num, ops_num, ops_num], name = 'ops')
    ops_type = pd.Series([d_ops_type, d_ops_type, d_ops_type], name = 'ops_type')

    to_add = pd.concat([y2020, value_type, value, ops, ops_type], axis=1)

    final_data = final_data.append(to_add, ignore_index = True)

for index, row in query_data_2021.iterrows():

    druid = row['operator'] + row['operator_2']
    ops_num = row['organizationUnit']

    ops_type_row = ops_type_data.loc[ops_type_data['index_ops'] == ops_num]
    d_ops_type = ops_type_row.iloc[0]['ops_type']

    dohod_row = dohod_data.loc[dohod_data['Индекс подразделения'] == ops_num]

    #dohod_row = dohod_data.loc[dohod_data['Индекс подразделения'] == ops_num]
    #if dohod_row.empty == True:
    #    d_value = 'None'
    #    d_ops_type = 'WIP'
    #else :
    #    d_value = dohod_row.iloc[0]['Количество ставок по ШР']
    #    d_ops_type = dohod_row.iloc[0]['Тип ОПС']
    
    y2020 = pd.Series(["Fri Jan 01 2021 03:00:00 GMT+0300 (Москва, стандартное время)", "Fri Jan 01 2021 03:00:00 GMT+0300 (Москва, стандартное время)", "Fri Jan 01 2021 03:00:00 GMT+0300 (Москва, стандартное время)"], name= 'year')
    value_type = pd.Series(["druid", "расчетная", "фактическая"], name = 'value_type')
    value = pd.Series([druid, row['employee_count'], d_value], name = 'value')
    ops = pd.Series([ops_num, ops_num, ops_num], name = 'ops')
    ops_type = pd.Series([d_ops_type, d_ops_type, d_ops_type], name = 'ops_type')

    to_add = pd.concat([y2020, value_type, value, ops, ops_type], axis=1)

    final_data = final_data.append(to_add, ignore_index = True)

final_data.to_excel(r'C:\Users\danil\Documents\Work\output.xlsx')