import pandas as pd
from slacker import Slacker
import pygsheets as pg
from datetime import date
from datetime import datetime
from datetime import timedelta

#Подключение к слаку
slack = Slacker('xoxb-886445230981-908663299460-AQ4l76vgeWvCRuisQHveVp5D')
#Подключение к гуглошиту логи
gc = pg.authorize(service_account_file= r'C:\Users\kurdyubov.n\Desktop\Untitled Folder\creditnails\creds.json')
sh = gc.open('logs')
log_file = sh[0]

#Вспомогательные функции
def condition_1_flag(df):
    if df.city == 'москва' and df.spent >= 2000.0:
        x = 1
    elif df.city != 'москва'and df.spent >=1000.0:
        x = 1
    else:
        x = 0
    return x
def condition_3_cpa(df):
    if df.lead!=0:
        x = (float(df.spent) / float(df.lead))
    else:
        x = 0
    return x
def condition_3_flag(df):
    if df.lead!=0:
        x = (float(df.spent) / float(df.lead)) / float(df.cpa_lim)
    else:
        x = 0
    return x

#Основные функции
def condition_1(df):
    """
    Если кампания потратила больше 1к в регионе и не принесла лидов, или 2к для Мск
    """
    cond = df.groupby(['campaign', 'city']).agg('sum').reset_index()
    cond['flag'] = cond.apply(condition_1_flag, axis=1)
    cond[cond['flag'] == 1].to_csv('condition_1.csv', encoding='ansi')
    slack.files.upload(file_='condition_1.csv', channels='#main-alfa', initial_comment='{} кампаний с высокими тратами имеют 0 лидов'.format(len(cond[cond['flag'] == 1])))
    log_file.insert_rows(0, 1, values=['condition_1', datetime.today().strftime("%Y-%m-%d_%H:%M:%S")])

def condition_2(df):
    """
    Расходы по яндексу и гуглу к 16:00
    """
    cond = df[df['dates'] == date.today()]
    a = cond[cond['system'] == 'direct'].spent.sum()
    b = cond[cond['system'] == 'adwords'].spent.sum()
    slack.chat.post_message('#main-alfa', 'Траты Яндекс: {}, траты Гугл: {}'.format(round(a,2), round(b,2)))
    log_file.insert_rows(0, 1, values=['condition_2', datetime.today().strftime("%Y-%m-%d_%H:%M:%S")])

def condition_3(df):
    """
    Превышение лимитов CPA
    """
    cpa = pd.read_csv('https://docs.google.com/spreadsheets/d/1PHb73knSLAEhmVpMddsjqIyYHJzY_K2Yp4C8zauxAoc/export?format=csv&gid=128476243')
    dff = pd.merge(df.groupby(['campaign', 'city']).agg('sum').reset_index(), cpa, 'left', on='city')
    dff['cpa_real'] = dff.apply(condition_3_cpa, axis=1)
    dff['flag'] = dff.apply(condition_3_flag, axis=1)
    dff[dff['flag'] > 1].to_csv('condition_3.csv', encoding='ansi')
    slack.files.upload(file_='condition_3.csv', channels='#main-alfa', initial_comment='{} кампаний первысили CPA по городу'.format(len(dff[dff['flag'] > 1])))
    log_file.insert_rows(0, 1, values=['condition_3', datetime.today().strftime("%Y-%m-%d_%H:%M:%S")])

def condition_4(df, plan_fact):
    """
    Траты первысили план на 50%
    """
    a = df[df['dates'] == date.today()].spent.sum()
    b = plan_fact[plan_fact['Дата'] == date.today().strftime("%d.%m.%Y")]['План на день (месяц)']
    if a > 1.5*float(b):
        slack.chat.post_message('#main-alfa', 'Перетраты!!! План на сегодня: {}, траты уже сейчас {}'.format(round(float(b),2), round(a,2)))
        log_file.insert_rows(0, 1, values=['condition_4', datetime.today().strftime("%Y-%m-%d_%H:%M:%S")])
    else:
        pass

def condition_5(df, plan_fact):
    """
    Потратили меньше плана
    """
    a = df[df['dates'] == (date.today() - timedelta(days=1))].spent.sum()
    b = plan_fact[plan_fact['Дата'] == (date.today() - timedelta(days=1)).strftime("%d.%m.%Y")]['План на день (месяц)']
    if a < 0.5*float(b):
        slack.chat.post_message('#main-alfa', 'Вчера недотрата. План на вчера: {}, траты вчера {}'.format(round(float(b),2), round(a,2)))
        log_file.insert_rows(0, 1, values=['condition_5', datetime.today().strftime("%Y-%m-%d_%H:%M:%S")])
    else:
        pass