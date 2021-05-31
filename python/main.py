import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from matplotlib import cm
from matplotlib.colors import to_hex
import numpy as np
import pandas as pd
from pymongo import MongoClient
import time

data = pd.read_pickle('data.pkl', compression='gzip')

unique_genres = np.unique(data['genres'])
colors = cm.gist_rainbow([i / len(unique_genres) for i in range(len(unique_genres))])
colors = {i: to_hex(j) for i, j in zip(unique_genres, colors)}

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options

features = {'runtimes': [0, 180],
            'rating': [0, 10],
            'budget sum': [0, 1e10],
            'gross sum': [0, 2e10],
            'profit': [0, 10],
            'profit mean': [0, 10],
            'screen width': [0.5, 3.5],
            'Sex & Nudity': [0, 1],
            'Violence & Gore': [0, 1],
            'Profanity': [0, 1],
            'Alcohol, Drugs & Smoking': [0, 1],
            'Frightening & Intense Scenes': [0, 1]}

app.layout = html.Div([
    html.H1(children='HoCiC'),

    html.Div(children='''
        History of Cinematography in Circles
    '''),

    html.Div([
        html.Div([
            dcc.Dropdown(
                id='xaxis-column',
                options=[{'label': i, 'value': i} for i in features],
                value='runtimes'),
            dcc.RadioItems(
                id='xaxis-type',
                options=[{'label': i, 'value': i} for i in ['constant', 'dynamic']],
                value='constant',
                labelStyle={'display': 'inline-block'}
            )
        ],
            style={'width': '48%', 'display': 'inline-block'}),

        html.Div([
            dcc.Dropdown(
                id='yaxis-column',
                options=[{'label': i, 'value': i} for i in features],
                value='rating'),
            dcc.RadioItems(
                id='yaxis-type',
                options=[{'label': i, 'value': i} for i in ['constant', 'dynamic']],
                value='constant',
                labelStyle={'display': 'inline-block'}
            )
        ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'})
    ]),

    dcc.Graph(id='graph'),

    dcc.Slider(
        id='year-slider',
        min=data['year'].min(),
        max=data['year'].max(),
        value=data['year'].min(),
        marks={str(year): str(year) if year % 5 == 0 else '' for year in data['year'].unique()},
        step=None
    )
])


@app.callback(
    Output('graph', 'figure'),
    Input('xaxis-column', 'value'),
    Input('yaxis-column', 'value'),
    Input('xaxis-type', 'value'),
    Input('yaxis-type', 'value'),
    Input('year-slider', 'value'))
def update_figure(x_feature, y_feature, x_type, y_type, selected_year):
    filtered_df = data[data['year'] == selected_year]

    color_discrete_sequence = [colors[i] for i in np.unique(filtered_df['genres'])]

    fig = px.scatter(filtered_df,
                     x=x_feature,
                     y=y_feature,
                     size="votes sum",
                     size_max=50,
                     color='genres',
                     color_discrete_sequence=color_discrete_sequence)
    if x_type == 'constant':
        fig.update_xaxes(range=features[x_feature])
    if y_type == 'constant':
        fig.update_yaxes(range=features[y_feature])
    fig.update_layout(title={'text': str(selected_year),
                             'x': 0.475})

    fig.update_layout(transition_duration=500)

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
