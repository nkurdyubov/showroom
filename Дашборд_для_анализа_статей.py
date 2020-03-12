# -*- coding: utf-8 -*-
import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.subplots as psp

df = pd.read_csv('Ссылка на таблицу со статьями')
#Удаление лишнего столбца, получаемого при загрузке из гугл-таб
df = df.drop('Unnamed: 0', 1)

external_stylesheets = ['assets/style.css']
app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
app.config.suppress_callback_exceptions = True

# Раскидываем элементы
app.layout = html.Div([
    # Формирование строки фильтров из дропдаунов
    html.Div([
        dcc.Dropdown(
            id='dropdown_authors',
            options=[{'label': author, 'value': author} for author in df['Автор'].unique()],
            multi=True,
            value=[author for author in df['Автор'].unique()],
            clearable=True,
            searchable=False),
        dcc.Dropdown(
            id='dropdown_categories',
            options=[{'label': cat, 'value': cat} for cat in df['Категория'].unique()],
            multi=True,
            value=[cat for cat in df['Категория'].unique()],
            clearable=True,
            searchable=False),
        dcc.Dropdown(
            id='dropdown_types',
            options=[{'label': types, 'value': types} for types in df['Тип'].unique()],
            multi=True,
            value=[types for types in df['Тип'].unique()],
            clearable=True,
            searchable=False),
        dcc.Dropdown(
            id='dropdown_rubricks',
            options=[{'label': rubrick, 'value': rubrick} for rubrick in df['Рубрика'].unique()],
            multi=True,
            value=[rubrick for rubrick in df['Рубрика'].unique()],
            clearable=True,
            searchable=False)
            ],
        style={'columnCount': 4},
        className='filterbar'),
    #Дататейбл
    html.Div(
        dash_table.DataTable(
            style_cell={
            'overflow': 'hidden',
            'textOverflow': 'ellipsis',
            'maxWidth': 0,
                },
            fixed_rows={ 'headers': True, 'data': 0 },
            columns=[
                {"name": i, "id": i, "deletable": True} for i in df.columns
                ],
            id='datatable-interactivity',
            data=df.to_dict('records'),
            editable=True,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            row_selectable="multi",
            row_deletable=True,
            selected_rows=[],
            )
        ),
    #Строка с информацией и кнопкой сброса селектов
    html.Div([
        #Количество статей всего/выбранных
        html.Div(id='info_string'),
        #Кнопка сбраса
        html.Button('DROP SELECT', id='button')
    ]),
    #Графики по статьям
    html.Div([
        #Общий график по статьям
        dcc.Graph(id='bar_plots'),
        #Данные по выбранной статье
        html.Div(id='selected')],
        style={'columnCount': 2},
        className='data_graphs'),
        
    #Диаграммы по таблице    
    html.Div(id='pie_plots',
             style={'columnCount': 3},
             className='data_pies')
    ])

#Обновление данных по фильтрам
@app.callback(
    Output('datatable-interactivity', 'data'), 
    [
        Input('dropdown_authors', 'value'),
        Input('dropdown_categories', 'value'),
        Input('dropdown_types', 'value'),
        Input('dropdown_rubricks', 'value'),
        ]
        )
def update_table(dropdown_authors,dropdown_categories,dropdown_types,dropdown_rubricks):
    """
    For user selections, return the relevant table
    """
    df_to_ret = df[df['Автор'].isin(dropdown_authors) & 
                   df['Категория'].isin(dropdown_categories) & 
                   df['Тип'].isin(dropdown_types) &   
                   df['Рубрика'].isin(dropdown_rubricks)]
    
    return df_to_ret.to_dict('records')

#Графики 
@app.callback(
    Output('bar_plots', "figure"),
    [Input('datatable-interactivity', "derived_virtual_data"),
     Input('datatable-interactivity', "derived_virtual_selected_rows")])
def update_graphs(rows, derived_virtual_selected_rows):
    if derived_virtual_selected_rows is None:
        derived_virtual_selected_rows = []

    dff = df if rows is None else pd.DataFrame(rows)

    colors = ['#ADFF2F' if i in derived_virtual_selected_rows else '#00FFFF'
              for i in range(len(dff))]
    columns = ["search_v", "search_u", 'referrer_v', 'referrer_u', 'social_v', 'social_u']
    fig = psp.make_subplots(rows=len(columns), cols=1, shared_xaxes=True, shared_yaxes=True,vertical_spacing=0.009,subplot_titles=[column for column in columns])#,vertical_spacing=0.009,horizontal_spacing=0.009)
    fig['layout']['clickmode'] = 'event+select'
    fig['layout']['width'] = 800
    fig['layout']['height'] = 1000
    #fig['layout']['autosize'] = True
    fig['layout']['margin'] = {'l': 30, 'r': 10, 'b': 10, 't': 50}
    fig['layout']['showlegend'] = False
    fig.update_xaxes(showticklabels=False)
    for column in columns:
        fig.append_trace({
                        'x': dff['Статья'],
                        "y": dff[column],
                        "type": "bar",
                        "marker": {"color": colors},
                        'text': column
                         }, row=columns.index(column)+1, col=1)
    return fig
       
#Пироги
@app.callback(
    Output('pie_plots', "children"),
    [Input('datatable-interactivity', "derived_virtual_data"),
     Input('datatable-interactivity', "derived_virtual_selected_rows")])
def update_graphs(rows, derived_virtual_selected_rows):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    if derived_virtual_selected_rows is None:
        derived_virtual_selected_rows = []

    dff = df if rows is None else pd.DataFrame(rows)

    colors = ['#ADFF2F' if i in derived_virtual_selected_rows else '#00FFFF'
              for i in range(len(dff))]

    return [
        dcc.Graph(
            id=column,
            figure={
                "data": [
                    {
                        "values": [dff[column][dff[column] == name].count() for name in dff[column].unique()],
                        "labels": [name for name in dff[column].unique()],
                        "type": "pie",
                        "marker": {"color": colors},
                    }
                ],
                "layout": {
                    'height': 600,
                    'width': 600,
                },
            },
        )
        # check if column exists - user may have deleted it
        # If `column.deletable=False`, then you don't
        # need to do this check.
        for column in ["Категория", "Тип", "Рублика"] if column in dff
    ]

#Выбор статей, которые выбрали ###ДОДЕЛАТЬ ФИЧУ С КНОПКОЙ_____________________№№!"№!№"
@app.callback(
    Output('datatable-interactivity', 'selected_rows'), 
    [Input('bar_plots', 'selectedData'),
     #Input('button', 'n_clicks'),
    ]
        )
def selected_point(data):
    sel = []
    if data is not None:
        selected_bars = [i['x'] for i in data['points']]
        sel = df[df["Статья"].isin(selected_bars)].index.tolist()
    return sel

#Детальная инфа о выбранных статьях
@app.callback(
    Output('selected', 'children'), 
    [Input('datatable-interactivity', 'selected_rows')]
        )
def selected_point(selected_rows):
    selected_df = None
    if selected_rows is not None:
        selected_df = df.drop(df.columns[[x for x in range(7,15)]], axis=1)
    return [html.Div([#Это список Div
            #Генерируется список из таблицы, графика и графика
        #Это список элементов
         #### Тут формирование таблица
            html.Table([
                html.Tbody([
                    html.Tr([
                        html.Td('Название статьи'),
                        html.Td(selected_df.loc[i,:]['Статья'])
                    ]),
                    html.Tr([
                        html.Td('Автор'),
                        html.Td(selected_df.loc[i,:]['Автор'])
                    ]),
                    html.Tr([
                        html.Td('Категория'),
                        html.Td(selected_df.loc[i,:]['Категория'])
                    ]),
                    html.Tr([
                        html.Td('Тип'),
                        html.Td(selected_df.loc[i,:]['Тип'])
                    ]),
                    html.Tr([
                        html.Td('Рубрика'),
                        html.Td(selected_df.loc[i,:]['Рублика'])
                    ])
                ]),
                
            ]
            ),#Тут конец описания таблицы, следующий элемент график
    ### ТУТ ИДЕТ ПОСТРОЕНИЕ ГРАФИКОВ
    dcc.Graph(
            figure={#О
                'data':[
                    {
                    'x': ['Визиты', 'Посетители'],
                    'y': [selected_df.loc[i,:]['search_v'], selected_df.loc[i,:]['search_u']],
                    'name': 'Поиск',
                    'type': 'bar'
                    },
                    {
                    'x': ['Визиты', 'Посетители'],
                    'y': [selected_df.loc[i,:]['referrer_v'], selected_df.loc[i,:]['referrer_u']],
                    'name': 'Ссылки на сайтах',
                    'type': 'bar'
                    },
                    {
                    'x': ['Визиты', 'Посетители'],
                    'y': [selected_df.loc[i,:]['social_v'], selected_df.loc[i,:]['social_u']],
                    'name': 'Соц. сети',
                    'type': 'bar'
                    }
                    ],
                'layout': {'barmode': 'stack'}
            }#Закрываю фигуру
            )#Закрываю граф
        ### ВОТ ТУТ ДОБАВИТЬ ЕЩЕ ОДИН ГРАФ С ИЗМЕНЕНИЯМ ВО ВРЕМЕНИ    
    ]#Закрываю children Дива
    )#Закрываю Div
     for i in selected_rows] # Окончание цикла
    
#Подсчет строк
@app.callback(
    Output('info_string', 'children'), 
    [
        Input('dropdown_authors', 'value'),
        Input('dropdown_categories', 'value'),
        Input('dropdown_types', 'value'),
        Input('dropdown_rubricks', 'value'),
        ]
        )
def update_table(dropdown_authors,dropdown_categories,dropdown_types,dropdown_rubricks):
    """
    For user selections, return the relevant table
    """
    df_to_ret = df[df['Автор'].isin(dropdown_authors) & 
                   df['Категория'].isin(dropdown_categories) & 
                   df['Тип'].isin(dropdown_types) &   
                   df['Рублика'].isin(dropdown_rubricks)]
    
    return 'Всего статей: {}; Выбрано статей: {}'.format(len(df), len(df_to_ret))

def drop_select(click):
    return []

if __name__ == '__main__':
    app.run_server(debug=True)
