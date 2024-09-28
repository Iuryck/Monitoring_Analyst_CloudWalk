import requests
import pandas as pd
import numpy as np
import duckdb
import json
import dash
from dash import html, Dash, dcc
import numpy as np
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


feed=[]

app = Dash(__name__)

app.layout = html.Div([
    html.H1(id='title'),
    dcc.Graph(id='livestream'),
    dcc.Graph(id='pie'),
    dcc.Interval(id='interval', interval=200)
])


def data_gen():
    df = pd.read_csv('transactions_2.csv')
    df2 = pd.read_csv('transactions_1.csv')
    
    roll_window = 15

    transactions = duckdb.query("""
        SELECT time, coalesce(approved,0) as approved, coalesce(denied,0) as denied, coalesce(failed,0) as failed, coalesce(reversed,0) as reversed, coalesce(refunded,0) as refunded, coalesce(processing,0) as processing,coalesce(backend_reversed, 0 ) as backend_reversed
        FROM (                      
            pivot df
            on status
            using sum(count)
        ) as df
        ORDER BY time
        """).df()
    
    transactions2 = duckdb.query("""
        SELECT time, coalesce(approved,0) as approved, coalesce(denied,0) as denied, coalesce(failed,0) as failed, coalesce(reversed,0) as reversed, coalesce(refunded,0) as refunded, coalesce(processing,0) as processing,coalesce(backend_reversed, 0 ) as backend_reversed
        FROM (                      
            pivot df2
            on status
            using sum(f0_)
        ) as df2
        ORDER BY time
        """).df()
    
    transactions = pd.concat([transactions, transactions2])
    
    transactions['denied_approved_roll30_corr'] = transactions.denied.rolling(roll_window).corr(transactions.approved) 
    transactions['reversed_approved_roll30_corr'] = transactions.reversed.rolling(roll_window).corr(transactions.approved) 
    transactions['failed_approved_roll30_corr'] = transactions.failed.rolling(roll_window).corr(transactions.approved) 
    transactions['processing_approved_roll30_corr'] = transactions.processing.rolling(roll_window).corr(transactions.approved) 
    transactions['refunded_approved_roll30_corr'] = transactions.refunded.rolling(roll_window).corr(transactions.approved) 
    transactions['backend_reversed_approved_roll30_corr'] = transactions.backend_reversed.rolling(roll_window).corr(transactions.approved) 


    transactions['denied_roll30_cumsum'] = transactions.denied.rolling(roll_window).sum()
    transactions['reversed_roll30_cumsum'] = transactions.reversed.rolling(roll_window).sum()
    transactions['failed_roll30_cumsum'] = transactions.failed.rolling(roll_window).sum()
    transactions['processing_roll30_cumsum'] = transactions.processing.rolling(roll_window).sum()
    transactions['refunded_roll30_cumsum'] = transactions.refunded.rolling(roll_window).sum()
    transactions['backend_reversed_roll30_cumsum'] = transactions.backend_reversed.rolling(roll_window).sum()


    transactions['denied_roll30_avg'] = transactions.denied.rolling(roll_window).mean()
    transactions['reversed_roll30_avg'] = transactions.reversed.rolling(roll_window).mean()
    transactions['failed_roll30_avg'] = transactions.failed.rolling(roll_window).mean()
    transactions['processing_roll30_avg'] = transactions.processing.rolling(roll_window).mean()
    transactions['refunded_roll30_avg'] = transactions.refunded.rolling(roll_window).mean()
    transactions['backend_reversed_roll30_avg'] = transactions.backend_reversed.rolling(roll_window).mean()

    transactions['denied_roll30_min'] = transactions.denied.rolling(roll_window).min()
    transactions['reversed_roll30_min'] = transactions.reversed.rolling(roll_window).min()
    transactions['failed_roll30_min'] = transactions.failed.rolling(roll_window).mean()
    transactions['processing_roll30_min'] = transactions.processing.rolling(roll_window).min()
    transactions['refunded_roll30_min'] = transactions.refunded.rolling(roll_window).min()
    transactions['backend_reversed_roll30_min'] = transactions.backend_reversed.rolling(roll_window).min()

    transactions['denied_roll30_max'] = transactions.denied.rolling(roll_window).max()
    transactions['reversed_roll30_max'] = transactions.reversed.rolling(roll_window).max()
    transactions['failed_roll30_max'] = transactions.failed.rolling(roll_window).max()
    transactions['processing_roll30_max'] = transactions.processing.rolling(roll_window).max()
    transactions['refunded_roll30_max'] = transactions.refunded.rolling(roll_window).max()
    transactions['backend_reversed_roll30_max'] = transactions.backend_reversed.rolling(roll_window).max()
    
    transactions = transactions.replace(np.nan, 0)
    
    time_list = [c for c in transactions.time.unique()]
    i=0
    while True:
        time = time_list[i]
        i+=1
       
        yield transactions.loc[transactions.time==time].to_dict('records')[0]
        
        try:
            time_list[i+1]
        except IndexError: i=0
            
            



gen = data_gen()




@app.callback( 
        Output('livestream','figure'),
        Output('title','children'),
        Output('pie','figure'),
        Input('interval','n_intervals')
        
          )
def update_figure(n_intervals):

    
    data = next(gen)    
        
    r = requests.post('http://127.0.0.1:5000/send_transactions', json=json.dumps(data))
    
    data = pd.DataFrame.from_records(data, index=[0])

    r = r.json()
    anomaly_data = pd.DataFrame.from_records(r['anomalies_dataframe'], index=data.time)
    detection_data = r['info_dict']
    data.time = pd.to_datetime(data['time'],format= '%Hh %M' ).dt.time
    print(detection_data)
    data.loc[:,'anomaly_failed'] = int(detection_data.get('failed').get('anomaly'))
    data.loc[:,'anomaly_denied'] = int(detection_data.get('denied').get('anomaly'))
    data.loc[:,'anomaly_reversed'] = int(detection_data.get('reversed').get('anomaly'))
    data.loc[:,'anomaly_approved'] = int(detection_data.get('approved').get('anomaly'))
    data.loc[:,'anomaly_refunded'] = int(detection_data.get('refunded').get('anomaly'))

    feed.append(data)    
    

    if len(feed) > 120:
        live_data=pd.concat(feed[-120:])
    else:
        live_data=pd.concat(feed)


    

    fig = make_subplots(specs=[[{"secondary_y": True}]])


  
    
    fig.add_trace(go.Scatter(x=live_data.time, y=live_data.approved, mode='lines', name='Approved', line_color='rgb(0, 188, 69)'))
    fig.add_trace(go.Scatter(x=live_data.time, y=live_data.denied, mode='lines', name='Denied', line_color='rgb(242, 8, 0)'))
    fig.add_trace(go.Scatter(x=live_data.time, y=live_data.reversed, mode='lines', name='Reversed', line_color='rgb(0, 84, 230)'))
    fig.add_trace(go.Scatter(x=live_data.time, y=live_data.failed, mode='lines', name='Failed', line_color='rgb(126, 05, 208)'))

     
 
    fig.add_trace(go.Bar(x=live_data.time, y=live_data.anomaly_failed, name='Anomaly Failed', opacity=0.5), secondary_y=True)

    fig.add_trace(go.Bar(x=live_data.time, y=live_data.anomaly_reversed, name='Anomaly Reversed', opacity=0.5), secondary_y=True)

    fig.add_trace(go.Bar(x=live_data.time, y=live_data.anomaly_denied, name='Anomaly Denied', opacity=0.5), secondary_y=True)   
    fig.add_trace(go.Bar(x=live_data.time, y=live_data.anomaly_approved, name='Anomaly Approved', opacity=0.5), secondary_y=True)  
    fig.add_trace(go.Bar(x=live_data.time, y=live_data.anomaly_refunded, name='Anomaly Denied', opacity=0.5), secondary_y=True)    
    
    fig.layout.yaxis2.update(showticklabels=False)

    fig.update_layout(xaxis_rangeslider_visible=False, height=400)

    if len(feed) >20:
        values = [np.sum(live_data.approved.values[-20:]),
                  np.sum(live_data.denied.values[-20:]),
                  np.sum(live_data.reversed.values[-20:]),
                  np.sum(live_data.failed.values[-20:])]
        names = ['Approved','Denied','Reversed','Failed']
    else:
        values = [np.sum(live_data.approved),
                  np.sum(live_data.denied),
                  np.sum(live_data.reversed),
                  np.sum(live_data.failed)]
        names = ['Approved','Denied','Reversed','Failed']


    
    pie_chart = px.pie(values=values, names=names, hole=.3, title='20min Window', color=names,
                        color_discrete_map={'Approved':'rgb(0, 188, 69)',
                                 'Denied':'rgb(242, 8, 0)',
                                 'Failed':'rgb(126, 05, 208)',
                                 'Reversed':'rgb(0, 84, 230)'})

    return fig, f'Monitoring - {data.time.values[0]}', pie_chart

if __name__ == '__main__':
   
    app.run_server(debug=True)

    

 

    

    

    
        



    


