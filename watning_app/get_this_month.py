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

def add_date(df):
    if df.date == df.collected_on:
        x = df.collected_on
    elif df.date is np.nan:
        x = df.collected_on
    else:
        x = df.date
    return x

def get_this_month():
    corina = psycopg2.connect(
    database="corina_prod", 
    user="report", 
    password="qpXR9BU95Sef", 
    host="139.162.222.133", 
    port="5432"
    )
    
    df_corina_weekly = pd.read_sql(
    "WITH cte AS (\
    select\
            r.client_id,\
            r.data->'session' AS session,\
            'call' as ct_type,\
            r.data->>'siteID' as siteid,\
            r.data->>'callerNumber' as phone,\
            to_date(r.data->>'date', 'DD/MM/YYYY HH24:MI:SS') as date,\
            date_trunc('month', TO_TIMESTAMP(r.data->>'date', 'DD/MM/YYYY HH24:MI:SS'))::date\
            || ' '\
            || (date_trunc('month', TO_TIMESTAMP(r.data->>'date', 'DD/MM/YYYY HH24:MI:SS'))+ '1 month'::INTERVAL - '1 days'::interval)::date as week,\
            r.data->>'utmSource' as source,\
            r.data->>'utmMedium' as medium,\
            r.data->>'medium' as ct_medium,\
            r.data->>'utmCampaign' as campaign,\
            r.data->>'duration' as duration,\
            r.data->>'source' as ct_source,\
            r.data->>'url' as url,\
            r.data->>'siteName' as site,\
            r.data->>'targetCall' AS target,\
            r.data->>'callTags' AS tags\
            from raw_communications r\
            where client_id in (7)\
            and r.data->'session' is null\
            and r.source_name = 'call_touch'\
            AND to_date(r.data->>'date', 'DD/MM/YYYY HH24:MI:SS') BETWEEN {dates}\
            UNION all \
    select\
                r.client_id,\
                r.data->'session' AS session,\
                'req' as ct_type,\
                r.data->>'siteID' as siteid,\
                r.data->'client'->'phones'->(0)->>'phoneNumber' as phone,\
                to_date(r.data->>'dateStr', 'DD/MM/YYYY HH24:MI:SS') as date,\
                date_trunc('week', TO_TIMESTAMP(r.data->>'dateStr', 'DD/MM/YYYY HH24:MI:SS'))::date\
                || ' '\
                || (date_trunc('week', TO_TIMESTAMP(r.data->>'dateStr', 'DD/MM/YYYY HH24:MI:SS'))+ '1 month'::INTERVAL - '1 days'::interval)::date as week,\
                r.data->'session'->>'utmSource' as source,\
                r.data->'session'->>'utmMedium' as medium,\
                r.data->'session'->>'medium' as ct_medium,\
                r.data->'session'->>'utmCampaign' as campaign,\
                '0' as duration,\
                r.data->'session'->>'source' as ct_source,\
                r.data->'session'->>'url' as url,\
                '' as site,\
                r.data->>'targetRequest' as target,\
                r.data->>'callTags' AS tags\
                from raw_communications r\
                where client_id in (7)\
                and r.data->'session' is not null\
                and r.source_name = 'call_touch'\
                AND to_date(r.data->>'dateStr', 'DD/MM/YYYY HH24:MI:SS') BETWEEN {dates}\
        ) \
    \
    SELECT * FROM cte".format(dates=BETWEEN_STRING), corina
    )

    corina.close()
    df1 = df_corina_weekly[(df_corina_weekly['ct_source'] == 'yandex') | (df_corina_weekly['ct_source'] == 'google') |
                       (df_corina_weekly['ct_source'] == 'Визитка Яндекс') | (df_corina_weekly['ct_source'] == 'Визитка Google')]
    df2 = df1[df1['ct_medium'] == 'cpc']
    df3 = df2[df2['target'] == 'true'].copy()
    df3['t'] = df3.apply(no_target, axis=1)
    df4 = df3[df3['t'] != 0]
    df5 = df4.copy()
    df5['rn'] = df5.sort_values('date', ascending=True).groupby('phone').cumcount() 
    df6 = df5[df5['rn'] == 0].copy()
    df6['lead'] = 1
    df6.campaign.replace('<не указано>', "Визитка", inplace=True)
    df8 = df6.groupby(['date', 'campaign']).agg('sum').reset_index().copy()
    df8['campaign'] = df8.campaign.str.lower()

    alice = psycopg2.connect(
    database="alice_prod", 
    user="reporter", 
    password="qpXR9BU95Sef", 
    host="172.104.143.86", 
    port="5432"
    )

    df_alice_weekly = pd.read_sql(
    "/*direct*/\
    select \
    s.collected_on as collected_on, \
    s.clicks as clicks, \
    s.impressions as impressions, \
    s.cost as spent, \
    a.login as account, \
    s.campaign_name as campaign, \
    s.campaign_id::text as campaign_id, \
    'Direct' as system, \
    date_trunc('week', s.collected_on )::date \
            || ' ' \
            || (date_trunc('week', s.collected_on )+ '6 days'::interval)::date as week \
    \
    from yd_campaign_reports s \
    join yd_agency_clients a on a.id = s.yd_agency_client_id \
    where lower(a.login) in ( \
        'kr-alfazdrav-msk', 'kr-alfazdrav-nn', 'kr-alfazdrav-sam', 'kr-alfazdrav-sar', 'kr-alfazdrav-perm', 'kr-alfazdrav-ros', 'kr-alfazdrav-ekb', 'kr-alfazdrav-mur', 'kr-alfazdrav-tyum', 'kr-alfazdrav-yar', 'kr-alfazdrav-kirov' \
    ) \
    AND \
    s.collected_on BETWEEN {dates}\
    \
    /*adwords*/ \
    union \
    select \
    s.collected_on as collected_on, \
    s.clicks as clicks, \
    s.impressions as impressions, \
    s.spend as spent, \
    a.name as account, \
    s.campaign_name as campaign, \
    s.external_campaign_id as campaign_id, \
    'Adwords' as system, \
    date_trunc('week', s.collected_on )::date \
            || ' ' \
            || (date_trunc('week', s.collected_on )+ '6 days'::interval)::date as week \
    from gaw_campaign_stats s \
    join gaw_clients a on a.id = s.gaw_client_id \
    where lower(a.name) in ( \
        'москва', 'пермь new', 'екатеринбург new', 'киров new', 'мурманск new', 'нижний новгород_new', 'ростов на дону new', 'самара new', 'саратов new', 'тюмень new', 'ярославль new' \
    ) \
    AND \
    s.collected_on BETWEEN {dates}".format(dates=BETWEEN_STRING), 
                 alice)
    alice.close()

    df_alice_weekly['campaign'] = df_alice_weekly.campaign.str.lower()
    alice_full = df_alice_weekly.groupby(['collected_on' , 'campaign']).agg('sum').reset_index()
    ids = df_alice_weekly[['campaign', 'campaign_id']].drop_duplicates()
    corina_digits = df8[df8['campaign'].str.isdigit() == True]
    corina_letters = df8[df8['campaign'].str.isdigit() == False]
    corina_digits_camp = pd.merge(corina_digits, ids, 'left', left_on=['campaign'], right_on=['campaign_id'])
    corina_digits_camp = corina_digits_camp.drop(columns=['campaign_x', 'campaign_id']).rename(columns={'campaign_y':'campaign'})
    cor_full = pd.concat([corina_letters, corina_digits_camp], sort=False).groupby(['date', 'campaign']).agg('sum').reset_index()
    result = pd.merge(cor_full, alice_full, 'outer', left_on=['date', 'campaign'], right_on=['collected_on', 'campaign'])
    result['dates'] = result.apply(add_date, axis=1)
    result.drop(columns=['date', 'collected_on'])
    return result
