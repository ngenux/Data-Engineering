import dash
import dash_html_components as html
import dash_core_components as dcc
import numpy as np
import pyodbc as odbc
import pandas as pd
from pandas import DataFrame
import dash_table

DB = {'servername': 'DESKTOP-UUN5P6A',
      'database': 'master'}

conn = odbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database']
                    + ';Trusted_Connection=yes')
query = """select * from sparkkafka"""
#cursor = conn.cursor()
#output = cursor.execute(query)
#df = DataFrame(output.fetchall())
#df.columns = output.keys()
#rows = cursor.fetchall()
#print(rows)
#colnames = [desc[0] for desc in cursor.description]
#print(colnames)
#df = pd.DataFrame(rows, columns=colnames)

#print(results)
df = pd.read_sql(query, conn)
print(df)

print(df.to_dict('records'))
#df = pd.read_sql_query(query, conn)

print(df.head())
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H4('Dashboard'),
    dcc.Interval('graph-update', interval=2000, n_intervals=0),
    #dash_table.DataTable(
    #    id='table',
    #    data=df.to_dict('records'),
    #    columns=[{"CityName": i, "Temperature": i} for i in df.columns])])
    dash_table.DataTable(
        id='table',
        data=df.to_dict('records'),
        columns=[{'name': 'CityName', 'id': 'CityName'},
                 {'name': 'Temperature', 'id': 'Temperature'}
                 ]
    )
])


@app.callback(dash.dependencies.Output('table', 'Data'), [dash.dependencies.Input('graph-update', 'n_intervals')])

def updateTable(n):
    #cursor = conn.cursor()
    #cmd = query
    #results = cursor.execute(cmd).fetchall()
    #df = pd.read_sql(query, conn)
    #cmd = query
    #results = cursor.execute(cmd).fetchall()
    #colnames = [desc[0] for desc in cursor.description]
    #df = pd.DataFrame(results, columns=colnames)
    df = pd.read_sql(query, conn)
    return df.to_dict('records')


if __name__ == '__main__':
    app.run_server()

# from dash.dependencies import Input, Output

# Example data (a circle).
# resolution = 20
# t = np.linspace(0, np.pi * 2, resolution)
# x, y = np.cos(t), np.sin(t)
# Example app.
# figure = dict(data=[{'x': [], 'y': []}], layout=dict(xaxis=dict(range=[-1, 1]), yaxis=dict(range=[-1, 1])))
# app = dash.Dash(__name__, update_title=None)  # remove "Updating..." from title
# app.layout = html.Div([dcc.Graph(id='graph', figure=figure), dcc.Interval(id="interval")])


# @app.callback(Output('graph', 'extendData'), [Input('interval', 'n_intervals')])
# def update_data(n_intervals):
#    index = n_intervals % resolution
#    # tuple is (dict of new data, target trace index, number of points to keep)
#    return dict(x=[[x[index]]], y=[[y[index]]]), [0], 10


# if __name__ == '__main__':
#    app.run_server()
