import pygsheets as pg
import pandas as pd
import numpy as np
from datetime import date

def conn():
    gc = pg.authorize(service_account_file= r'Сервисный аккаунт для работы с АПИ гуглтаблиц')
    sh = gc.open('План факт universal')
    wks = sh[1]
    return wks

def update_fact(df):
    wks = conn()
    leads = df.groupby('dates').agg('sum').iloc[date.today().day-2, :]['lead']
    spent = df.groupby('dates').agg('sum').iloc[date.today().day-2, :]['spent']
    wks.update_value("H{}".format(date.today().day), spent)
    wks.update_value("O{}".format(date.today().day), leads)
    plan_fact = pd.read_csv('ГУГЛОТАБЛИЦА С ОБНОВЛЯЕМЫМ ПЛАН_ФАКТОМ')
    return plan_fact

def upd_plan(df, column_update=None,column_plan=None, column_fact=None, start_range=None):
    wks = conn()
    tail = 0
    res = pd.DataFrame()
    for period in df['Номер периода'].unique():
        dff = df[df['Номер периода'] == period]
        l = len(dff)
        i = 0
        while (l-i) > 0:
            if i == 0:
                dff.iloc[i, column_update] = (dff[column_plan].sum() + tail) / (l - i)
            elif i!=0 and str(dff.iloc[i-1, column_fact]) != 'nan':
                dff.iloc[i,column_update] = (dff[column_plan].sum() + tail - dff.iloc[:i, column_fact].sum() + tail) / (l - i)
            else:
                dff.iloc[i,column_update] = dff.iloc[i-1,column_update]
            i += 1
        tail =  dff[column_plan].sum() - dff.iloc[:, column_fact].sum()
        res = pd.concat([res, dff])
    values = res.iloc[:, column_update].values
    wks.update_values(start_range, values.reshape(len(values), 1).tolist())
