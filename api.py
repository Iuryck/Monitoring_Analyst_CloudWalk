from flask import Flask, request, jsonify
import datetime
import requests
import duckdb
import pandas as pd
import joblib
import numpy as np
import pickle 
from scipy.special import softmax
from flask_apscheduler import APScheduler
import pprint

app = Flask(__name__)

# Load Models
scaler = joblib.load('Models\\scaler.save')
clf_linear = joblib.load('Models\\linearR_denied.save')
pca = joblib.load('Models\\pca.save')

# For warning teams about anomalies in data
anomaly_warn = False
anomaly_types = []
fictional_url = None # dummy variable for url to post warnings to monitoring team

with open('Models\\means_dict.pkl', 'rb') as f:
    means_dict = pickle.load(f)


def warn_anomalies():
    
    if anomaly_warn:
      headers = {
          'message': 'Anomalies found in POS data, please check dashboards for insights',
          'anomaly_types': ','.join(anomaly_types),
          'auth_key': 'None'
      }   

      #Removing duplicates 
      anomaly_types = list(dict.fromkeys(anomaly_types))
      # fictional url will doesn't exist, skip any errors
      try: 
        requests.post(fictional_url, headers=headers)
        anomaly_types=[]
      except: pass
    
    else:pass


def detect_anomalies(data:pd.DataFrame):
    anomaly_dict = {}

    columns = data.columns 
    data = scaler.transform(data)
    pca_transformed = pca.transform(data)
    pca_inversed = pca.inverse_transform(pca_transformed)
    data = (data - pca_inversed)**2
    data.columns = columns
    softmax_data = pd.DataFrame(softmax(data), columns=columns)
    
    anomaly_df = pd.DataFrame(columns=data.columns)
    
    for col in columns:
        mean = means_dict.get(col).get('mean')
        std = means_dict.get(col).get('std')

        sigma3 = mean + 3*std
        sgima3_negative  = mean - 3*std

        value = data[col].values[0]
        key = col.split('_')[0]
        pct = softmax_data[col].values[0]

        if  value >= sigma3 or value <= sgima3_negative: 
          anomaly = True
          anomaly_warn = True
          anomaly_types.append(key)

        elif pct>0.015:
          anomaly = True
          anomaly_warn = True
          anomaly_types.append(key)
            
        else: anomaly = False

        anomaly_df.loc[0, col] = pct

        anomaly_dict.update({key: {'anomaly':anomaly, 'value':value,'pct':pct}})
    
    final_dict = {}
    final_dict.update({'info_dict':anomaly_dict})
    final_dict.update({'anomalies_dataframe':anomaly_df.to_dict('records')[0]})
    

    return final_dict


@app.route('/send_transactions', methods=['POST'])
def send_transactions():
    transactions = request.json
    
    df = pd.json_normalize(eval(transactions)) 

    del transactions

    epsilon = 0.001

    transactions = duckdb.query(f"""
                    SELECT df.*, tt.total as total
                      , (failed/total)*100 as failed_pct
                      , (reversed/total)*100 as reversed_pct
                      , (denied/total)*100 as denied_pct
                      , (processing/total)*100 as processing_pct
                      , (refunded/total)*100 as refunded_pct
                      , (backend_reversed/total)*100 as backend_reversed_pct
                      , (failed/(approved+{epsilon})) as failed_approved
                      , (reversed/(approved+{epsilon})) as reversed_approved
                      , (denied/(approved+{epsilon})) as denied_approved      
                      , (processing/(approved+{epsilon})) as processing_approved   
                      , (refunded/(approved+{epsilon})) as refunded_approved 
                      , (backend_reversed/(approved+{epsilon})) as backend_reversed_approved            
                      , POWER(denied,2) as denied_sqr
                      , POWER(reversed,2) as reversed_sqr
                      , POWER(processing,2) as processing_sqr
                      , POWER(refunded,2) as refunded_sqr
                      , POWER(backend_reversed,2) as backend_reversed_sqr
                      , POWER(failed,2) as failed_sqr
                      , POWER(denied,0.5) as denied_sqroot
                      , POWER(reversed,0.5) as reversed_sqroot
                      , POWER(processing,0.5) as processing_sqroot
                      , POWER(refunded,0.5) as refunded_sqroot
                      , POWER(backend_reversed,0.5) as backend_reversed_sqroot
                      , POWER(failed,0.5) as failed_sqroot
                      , denied_approved_roll30_corr, reversed_approved_roll30_corr, failed_approved_roll30_corr, processing_approved_roll30_corr, refunded_approved_roll30_corr, backend_reversed_approved_roll30_corr
                      , denied_roll30_cumsum, reversed_roll30_cumsum, failed_roll30_cumsum, processing_roll30_cumsum, refunded_roll30_cumsum, backend_reversed_roll30_cumsum
                      , denied_roll30_avg, reversed_roll30_avg, failed_roll30_avg, processing_roll30_avg, refunded_roll30_avg, backend_reversed_roll30_avg
                      , denied_roll30_min, reversed_roll30_min, failed_roll30_min, processing_roll30_min, refunded_roll30_min, backend_reversed_roll30_min
                      , denied_roll30_max, reversed_roll30_max, failed_roll30_max, processing_roll30_max, refunded_roll30_max, backend_reversed_roll30_max
                      , dummies.* EXCLUDE (time)
                    FROM df
                    LEFT JOIN (select time, (approved+denied+failed+reversed+processing+refunded) as total
                                from df) as tt                     
                    ON tt.time = df.time
                    LEFT JOIN (select time
                      ,IF ( CAST(df.time as STRING)[:2] == '00',1,0) as hour_00
                      ,IF ( CAST(df.time as STRING)[:2] == '01',1,0) as hour_01
                      ,IF ( CAST(df.time as STRING)[:2] == '02',1,0) as hour_02
                      ,IF ( CAST(df.time as STRING)[:2] == '03',1,0) as hour_03
                      ,IF ( CAST(df.time as STRING)[:2] == '04',1,0) as hour_04
                      ,IF ( CAST(df.time as STRING)[:2] == '05',1,0) as hour_05
                      ,IF ( CAST(df.time as STRING)[:2] == '06',1,0) as hour_06
                      ,IF ( CAST(df.time as STRING)[:2] == '07',1,0) as hour_07
                      ,IF ( CAST(df.time as STRING)[:2] == '08',1,0) as hour_08
                      ,IF ( CAST(df.time as STRING)[:2] == '09',1,0) as hour_09
                      ,IF ( CAST(df.time as STRING)[:2] == '10',1,0) as hour_10
                      ,IF ( CAST(df.time as STRING)[:2] == '11',1,0) as hour_11
                      ,IF ( CAST(df.time as STRING)[:2] == '12',1,0) as hour_12
                      ,IF ( CAST(df.time as STRING)[:2] == '13',1,0) as hour_13
                      ,IF ( CAST(df.time as STRING)[:2] == '14',1,0) as hour_14
                      ,IF ( CAST(df.time as STRING)[:2] == '15',1,0) as hour_15
                      ,IF ( CAST(df.time as STRING)[:2] == '16',1,0) as hour_16
                      ,IF ( CAST(df.time as STRING)[:2] == '17',1,0) as hour_17
                      ,IF ( CAST(df.time as STRING)[:2] == '18',1,0) as hour_18
                      ,IF ( CAST(df.time as STRING)[:2] == '19',1,0) as hour_19
                      ,IF ( CAST(df.time as STRING)[:2] == '20',1,0) as hour_20
                      ,IF ( CAST(df.time as STRING)[:2] == '21',1,0) as hour_21
                      ,IF ( CAST(df.time as STRING)[:2] == '22',1,0) as hour_22
                      ,IF ( CAST(df.time as STRING)[:2] == '23',1,0) as hour_23
                        from df
                        group by time) as dummies
                    ON dummies.time = df.time
    """).df() 

    roll_window = 15

    

    # Passing copy of data, remove data not used by models
    X = transactions.drop(['time'],axis=1)


    # Calculate distance from correlation denied X approved
    
    x_approved = np.array(X.approved).reshape(-1,1)
    X['denied_sqr_distance'] = (X.denied - clf_linear.predict(x_approved))**2

    # Ordering columns from moment of fit
    X = X[['approved', 'backend_reversed', 'denied', 'failed', 'processing',
       'refunded', 'reversed', 'total', 'denied_approved', 'denied_pct',
       'reversed_approved', 'reversed_pct', 'failed_approved', 'failed_pct',
       'processing_approved', 'processing_pct', 'refunded_approved',
       'refunded_pct', 'backend_reversed_approved', 'backend_reversed_pct',
       'denied_sqr', 'reversed_sqr', 'failed_sqr', 'processing_sqr',
       'refunded_sqr', 'backend_reversed_sqr', 'denied_sqroot',
       'reversed_sqroot', 'failed_sqroot', 'processing_sqroot',
       'refunded_sqroot', 'backend_reversed_sqroot',
       'denied_approved_roll30_corr', 'reversed_approved_roll30_corr',
       'failed_approved_roll30_corr', 'processing_approved_roll30_corr',
       'refunded_approved_roll30_corr',
       'backend_reversed_approved_roll30_corr', 'denied_roll30_cumsum',
       'reversed_roll30_cumsum', 'failed_roll30_cumsum',
       'processing_roll30_cumsum', 'refunded_roll30_cumsum',
       'backend_reversed_roll30_cumsum', 'denied_roll30_avg',
       'reversed_roll30_avg', 'failed_roll30_avg', 'processing_roll30_avg',
       'refunded_roll30_avg', 'backend_reversed_roll30_avg',
       'denied_roll30_min', 'reversed_roll30_min', 'failed_roll30_min',
       'processing_roll30_min', 'refunded_roll30_min',
       'backend_reversed_roll30_min', 'denied_roll30_max',
       'reversed_roll30_max', 'failed_roll30_max', 'processing_roll30_max',
       'refunded_roll30_max', 'backend_reversed_roll30_max',
       'denied_sqr_distance', 'hour_00', 'hour_01', 'hour_02', 'hour_03',
       'hour_04', 'hour_05', 'hour_06', 'hour_07', 'hour_08', 'hour_09',
       'hour_10', 'hour_11', 'hour_12', 'hour_13', 'hour_14', 'hour_15',
       'hour_16', 'hour_17', 'hour_18', 'hour_19', 'hour_20', 'hour_21',
       'hour_22', 'hour_23']]
    

    


    results = detect_anomalies(X)
    

    return jsonify(results)
    
    

app.run(threaded=True)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()
scheduler.add_job(id='anomaly-monitoring', func=warn_anomalies, trigger='interval', seconds=60)