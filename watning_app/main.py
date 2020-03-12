import pandas as pd
from datetime import date
from datetime import datetime
from datetime import timedelta
from get_data import get_corina_alice
from slacker import Slacker
import pygsheets as pg
import plan
import conditions

slack = Slacker('xoxb-886445230981-908663299460-AQ4l76vgeWvCRuisQHveVp5D')

gc = pg.authorize(service_account_file= r'C:\Users\kurdyubov.n\Desktop\Untitled Folder\creditnails\creds.json')
sh = gc.open('logs')
wks = sh[0]

#Собираем данные из баз
df = get_corina_alice()
#Обновляем план-факт
plan.upd_plan(plan.update_fact(df), 10, 'План к периоду l', 14, "K2") #Leads
plan.upd_plan(plan.update_fact(df), 3, 'План к периоду', 7, "D2") #Spent
plan_fact = plan.update_fact(df)
#Вырезка из журнала логов
check = pd.DataFrame(wks.get_values("A1", "B50"))

try:

    try:
        #cond1
        last_time = datetime.strptime(check[check[0] == 'condition_1'].iloc[0,1], "%Y-%m-%d_%H:%M:%S")
        if last_time.date() != date.today() and date.today().isoweekday() == 1:
            conditions.condition_1(df)
        slack.chat.post_message('@USENGJ6CC', 'Условие 1 ок', link_names=True, )
    except:
        slack.chat.post_message('@USENGJ6CC', 'Условие 1 проблема', link_names=True, )
        pass

    try:
        #cond2
        last_time = datetime.strptime(check[check[0] == 'condition_2'].iloc[0,1], "%Y-%m-%d_%H:%M:%S")
        if last_time.date() != date.today() and datetime.today().time().hour >= 16:
            conditions.condition_2(df)
        slack.chat.post_message('@USENGJ6CC', 'Условие 2 ок', link_names=True, )
    except:
        slack.chat.post_message('@USENGJ6CC', 'Условие 2 проблема', link_names=True, )
        pass
    
    try:   
        #cond3
        last_time = datetime.strptime(check[check[0] == 'condition_3'].iloc[0,1], "%Y-%m-%d_%H:%M:%S")
        if last_time.date() != date.today() and date.today().isoweekday() == 1:
            conditions.condition_3(df)
        slack.chat.post_message('@USENGJ6CC', 'Условие 3 ок', link_names=True, )
    except:
        slack.chat.post_message('@USENGJ6CC', 'Условие 3 проблема', link_names=True, )
        pass

    try:
    #cond4
        last_time = datetime.strptime(check[check[0] == 'condition_4'].iloc[0,1], "%Y-%m-%d_%H:%M:%S")
        if last_time.date() != date.today():
            conditions.condition_4(df, plan_fact)
        slack.chat.post_message('@USENGJ6CC', 'Условие 4 ок', link_names=True, )
    except:
        slack.chat.post_message('@USENGJ6CC', 'Условие 4 проблема', link_names=True, )
        pass

    try:
    #cond5
        last_time = datetime.strptime(check[check[0] == 'condition_5'].iloc[0,1], "%Y-%m-%d_%H:%M:%S")
        if last_time.date() != date.today():
            conditions.condition_5(df, plan_fact)
        slack.chat.post_message('@USENGJ6CC', 'Условие 5 ок', link_names=True, )
    except:
        slack.chat.post_message('@USENGJ6CC', 'Условие 5 проблема', link_names=True, )
        pass

    slack.chat.post_message('@USENGJ6CC', 'Все условия сработали', link_names=True, )
except:
    slack.chat.post_message('@USENGJ6CC', 'Хуево', link_names=True, )