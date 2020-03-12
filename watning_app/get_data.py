import pandas as pd
import numpy as np
from datetime import date
#Либка для работы с PostgreSQL
import psycopg2

BETWEEN_STRING = "'{}' AND '{}'".format(date.today().replace(day=1).strftime('%Y-%m-%d'), date.today().strftime('%Y-%m-%d'))

def no_target(df):
    if df['tags'] == None:
        x = 2
    elif "нецелевой" in df['tags']:
        x = 0
    
    else:
        x = 1
    return x

def add_system_corina(df):
    if 'ndex' in df.ct_source or 'ндекс' in df.ct_source:
        x = 'direct'
    elif 'ogle' in df.ct_source:
        x = 'adwords'
    else:
        x = None
    return x

def add_city_corina(df):
    if df['ct_type'] == 'call':
        x = df['site'].split(' ')[1]
    else:    
        if df['url'] == None:
            x = None
        elif df['url'].startswith('https://www.site.ru') or df['url'].startswith('https://site.ru') :
            x = 'Москва'
        elif 'perm' in df['url']:
            x = 'пермь'
        elif 'saratov' in df['url']:
            x = 'саратов'
        elif 'yaroslavl' in df['url']:
            x = 'ярославль'
        elif 'novgorod' in df['url']:
            x = 'нижний'
        elif 'kirov' in df['url']:
            x = 'киров'
        elif 'rostov' in df['url']:
            x = 'ростов-на-дону'
        elif 'tumen' in df['url']:
            x = 'тюмень'
        elif 'murmansk' in df['url']:
            x = 'мурманск'
        elif 'samara' in df['url']:
            x = 'самара'
        elif 'ekaterinburg' in df['url']:
            x = 'екатеринбург'    
        else: x = None
    return x

def add_city_alice(df):
    if 'Москва' in df['account'] or 'msk' in df['account']:
        x = 'москва'
    elif 'Киров' in df['account'] or 'kirov' in df['account']:
        x = 'киров'
    elif 'Пермь' in df['account'] or 'perm' in df['account']:
        x = 'пермь'
    elif 'Екатеринбург' in df['account'] or 'ekb' in df['account']:
        x = 'екатеринбург'
    elif 'Мурманск' in df['account'] or 'mur' in df['account']:
        x = 'мурманск'
    elif 'Новгород' in df['account'] or 'nn' in df['account']:
        x = 'нижний'
    elif 'Ростов' in df['account'] or 'ros' in df['account']:
        x = 'ростов-на-дону'
    elif 'Самара' in df['account'] or 'sam' in df['account']:
        x = 'самара'
    elif 'Саратов' in df['account'] or 'sar' in df['account']:
        x = 'саратов'
    elif 'Тюмень' in df['account'] or 'tyum' in df['account']:
        x = 'тюмень'
    elif 'Ярославль' in df['account'] or 'yar' in df['account']:
        x = 'ярославль'
    else:
        x = None
    return x

def add_date(df):
    if df.date == df.collected_on:
        x = df.collected_on
    elif df.date is np.nan:
        x = df.collected_on
    else:
        x = df.date
    return x

def get_corina():
    corina = psycopg2.connect(
    """
    Подключение к базе звонков
    """
    )

    df_corina = pd.read_sql(
    """WITH cte AS (
    select
            r.client_id,
            r.data->'session' AS session,
            'call' as ct_type,
            r.data->>'siteID' as siteid,
            r.data->>'callerNumber' as phone,
            to_date(r.data->>'date', 'DD/MM/YYYY HH24:MI:SS') as date,
            date_trunc('month', TO_TIMESTAMP(r.data->>'date', 'DD/MM/YYYY HH24:MI:SS'))::date
            || ' '
            || (date_trunc('month', TO_TIMESTAMP(r.data->>'date', 'DD/MM/YYYY HH24:MI:SS'))+ '1 month'::INTERVAL - '1 days'::interval)::date as week,
            r.data->>'utmSource' as source,
            r.data->>'utmMedium' as medium,
            r.data->>'medium' as ct_medium,
            r.data->>'utmCampaign' as campaign,
            r.data->>'duration' as duration,
            r.data->>'source' as ct_source,
            r.data->>'url' as url,
            r.data->>'siteName' as site,
            r.data->>'targetCall' AS target,
            r.data->>'callTags' AS tags
            from raw_communications r
            where client_id in (7)
            and r.data->'session' is null
            and r.source_name = 'call_touch'
            AND to_date(r.data->>'date', 'DD/MM/YYYY HH24:MI:SS') BETWEEN {dates}
            UNION all 
    select\
                r.client_id,
                r.data->'session' AS session,
                'req' as ct_type,
                r.data->>'siteID' as siteid,
                r.data->'client'->'phones'->(0)->>'phoneNumber' as phone,
                to_date(r.data->>'dateStr', 'DD/MM/YYYY HH24:MI:SS') as date,
                date_trunc('week', TO_TIMESTAMP(r.data->>'dateStr', 'DD/MM/YYYY HH24:MI:SS'))::date
                || ' '
                || (date_trunc('week', TO_TIMESTAMP(r.data->>'dateStr', 'DD/MM/YYYY HH24:MI:SS'))+ '1 month'::INTERVAL - '1 days'::interval)::date as week,
                r.data->'session'->>'utmSource' as source,
                r.data->'session'->>'utmMedium' as medium,
                r.data->'session'->>'medium' as ct_medium,
                r.data->'session'->>'utmCampaign' as campaign,
                '0' as duration,
                r.data->'session'->>'source' as ct_source,
                r.data->'session'->>'url' as url,
                '' as site,
                r.data->>'targetRequest' as target,
                r.data->>'callTags' AS tags
                from raw_communications r
                where client_id in (7)
                and r.data->'session' is not null
                and r.source_name = 'call_touch'
                AND to_date(r.data->>'dateStr', 'DD/MM/YYYY HH24:MI:SS') BETWEEN {dates}
        ) 
    
    SELECT * FROM cte""".format(dates=BETWEEN_STRING), corina
    )

    corina.close()

    return df_corina

def get_alice():
    alice = psycopg2.connect(
    """
    Подключение к базе рекламных кабинетов
    """
    )

    df_alice = pd.read_sql(
    """/*direct*/
    select 
    s.collected_on as collected_on, 
    s.clicks as clicks, 
    s.impressions as impressions, 
    s.cost as spent, 
    a.login as account, 
    s.campaign_name as campaign, 
    s.campaign_id::text as campaign_id, 
    'Direct' as system, 
    date_trunc('week', s.collected_on )::date 
            || ' ' 
            || (date_trunc('week', s.collected_on )+ '6 days'::interval)::date as week 
    
    from yd_campaign_reports s 
    join yd_agency_clients a on a.id = s.yd_agency_client_id 
    where lower(a.login) in ( 
        'kr-msk', 'kr-nn', 'kr-sam', 'kr-sar', 'kr-perm', 'krv-ros', 'kr-ekb', 'kr-mur', 'kr-tyum', 'kr-yar', 'krkirov' 
    ) 
    AND 
    s.collected_on BETWEEN {dates}
   
    /*adwords*/ 
    union 
    select 
    s.collected_on as collected_on, 
    s.clicks as clicks, 
    s.impressions as impressions, 
    s.spend as spent, 
    a.name as account, 
    s.campaign_name as campaign, 
    s.external_campaign_id as campaign_id, 
    'Adwords' as system, 
    date_trunc('week', s.collected_on )::date 
            || ' ' 
            || (date_trunc('week', s.collected_on )+ '6 days'::interval)::date as week 
    from gaw_campaign_stats s 
    join gaw_clients a on a.id = s.gaw_client_id 
    where lower(a.name) in ( 
        'москва', 'пермь new', 'екатеринбург new', 'киров new', 'мурманск new', 'нижний новгород_new', 'ростов на дону new', 'самара new', 'саратов new', 'тюмень new', 'ярославль new' 
    ) 
    AND 
    s.collected_on BETWEEN {dates}""".format(dates=BETWEEN_STRING), 
                 alice)

    alice.close()

    return df_alice


def get_corina_alice():
    #Получаем даные из БД
    corina = get_corina()
    alice = get_alice()
    ### Обработка корины
    #Фильтрация выгрузки по источнику
    df1 = corina[(corina['ct_source'] == 'yandex') | (corina['ct_source'] == 'google') |
                 (corina['ct_source'] == 'Визитка Яндекс') | (corina['ct_source'] == 'Визитка Google')]
    #Фильтрация выгрузки по medium==cpc
    df2 = df1[df1['ct_medium'] == 'cpc']
    #Фильтрация выгрузки по целевым звонкам
    df3 = df2[df2['target'] == 'true'].copy()
    #Применяется вспом. функц. no_target, создается столбец t с результатом выполнения функции. Если вернулся 0 - значит, есть тег "нецелевой"
    df3['t'] = df3.apply(no_target, axis=1)
    #Фильтрация по тегу "нецелевой"
    df4 = df3[df3['t'] != 0]
    #Подготовка к дальнейшей работе
    df5 = df4.reset_index().copy()
    #Определяем номер звонка (заявки) за рассматриваемый период. Создается новый столбец на основе таблицы,
    #сгруппированной по номеру телефона, при этом внутри каждой группы сортируются строки по дате,
    #после чего строки нумеруются, начиная с нуля.
    #В столбец возвращается номер строки внутри группы
    df5['rn'] = df5.sort_values('date', ascending=True).groupby('phone').cumcount()
    #Применяется функция add_system_corina. В новый столбец записывается система
    df5['system'] = df5.apply(add_system_corina, axis=1)
    #Отфильтрвываем строки с телефонами, дата которых первая за период
    df6 = df5[df5['rn'] == 0].copy()
    #Каждая строка - один лид. Создается столбец lead со значениям 1
    df6['lead'] = 1
    #Заменяем неуказанные кампании Визиткой
    df6.campaign.replace('<не указано>', "Визитка", inplace=True)
    df6.campaign.replace('<не заполнено>', "Визитка", inplace=True)
    df7 = df6.copy()
    #Применяем функцию add_city_corina, создается столбец city
    df7['city'] = df7.apply(add_city_corina, axis=1)
    #понижаем регистр стобца city
    df7['city'] = df7.city.str.lower()
    #Группируем выгрузку. Получаем уникальные строки с сочетанием даты, кампании, системы и города, остальные значения (например, lead) суммируются
    df8 = df7.groupby(['date', 'campaign', 'system', 'city']).agg('sum').reset_index().copy()
    #Понижаем регистр столбца с кампаниями
    df8['campaign'] = df8.campaign.str.lower()
    ### Обработка алисы
    #Понижаем регистр
    alice['campaign'] = alice.campaign.str.lower()
    #Используем функцию add_city_alice
    alice['city'] = alice.apply(add_city_alice, axis=1)
    alice['system'] = alice['system'].str.lower()
    #Группируем алису по дате, кампании, системе и городу. Остальные значения (клики, показы, траты) суммируются
    alice_full = alice.groupby(['collected_on' , 'campaign', 'system', 'city']).agg('sum').reset_index()
    ### Дописать названия кампаний в корину
    ### Т.к. в корине в поле campaign присутствуют значения campaign_id, необходимо заменгить их campaign_name
    #Собираем из алисы справочник уникальных сочетаний campaign_name & camp_id
    ids = alice[['campaign', 'campaign_id']].drop_duplicates()
    #Разбиваем корину на два фрейма: где в кампаниям только цифры (т.е. айди) и все остальное
    corina_digits = df8[df8['campaign'].str.isdigit() == True]
    corina_letters = df8[df8['campaign'].str.isdigit() == False]
    #Мерджим фрем с цифрами со справочником из алисы
    corina_digits_camp = pd.merge(corina_digits, ids, 'left', left_on=['campaign'], right_on=['campaign_id'])
    #Причесываем и переименовываем столбцы
    corina_digits_camp = corina_digits_camp.drop(columns=['campaign_x', 'campaign_id']).rename(columns={'campaign_y':'campaign'})
    #Конкатенируем переделанную таблицу с цифрами (в которой теперь все нормально) и таблицу со всем остальным, и получаем
    #Итоговую таблицу, которую еще раз группироуем по дате, кампании, системе и городу чтоб скоратить дубли строк
    cor_full = pd.concat([corina_letters, corina_digits_camp], sort=False).groupby(['date', 'campaign', 'system', 'city']).agg('sum').reset_index()
    ###Получаем результат
    result = pd.merge(cor_full, alice_full, 'outer', left_on=['date', 'campaign', 'system', 'city'], right_on=['collected_on', 'campaign', 'system', 'city'])
    #Собираем новый столбец с датами, применяем функцию add_date
    result['dates'] = result.apply(add_date, axis=1)
    #Удаляем старые столбцы с датами
    result.drop(columns=['date', 'collected_on'])
    return result
