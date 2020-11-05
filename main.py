# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 12:08:52 2020

@author: gilen
"""


from google.cloud import storage
import logging as log
import datetime
import time
import json
import requests
import os
import pandas as pd
import numpy as np
from datetime import datetime

import logging
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq

def get_analytic_data():
    url = f'https://economia.awesomeapi.com.br/json/daily/USD-BRL/15'

    for i in range(3):
        try:
            response = requests.request("GET", url)
            decoded_data=response.text.encode().decode('utf-8-sig')
            data = json.loads(decoded_data)
            listremovecolumns = ['code', 'codein', 'name', 'create_date']
            
            df = pd.DataFrame(data)
            df['remove'] = df.apply(lambda x: type(x['code'])==type(np.nan), axis = 1)
            dfaux = df[df['remove']!=True][listremovecolumns]
            df['datetime'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(int(x)))
            dfaux['date'] = dfaux['create_date'].apply(lambda x: datetime.date(datetime.strptime(x, '%Y-%m-%d %H:%M:%S')))
            listremovecolumns.append('remove')
            df = df.drop(columns=listremovecolumns)
            break
        except requests.exceptions.RequestException:
            log.exception("Error {} when trying to get response!".format(str(i + 1)))
            time.sleep(1)

    return df, dfaux

def save_bigquery(sm_table, dataset, table_name):
    """Import a csv file into BigQuery"""
    logging.info(table_name)
    client = bigquery.Client()
    # if os.environ.get('PRODUCTION'):
    #     client = bigquery.Client()
    # else:
    #     client = bigquery.Client.from_service_account_json("storage2bq.json")
    table_ref = client.dataset(dataset).table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    load_job = client.load_table_from_dataframe(sm_table,
                                                table_ref,
                                                job_config=job_config)
    # Waits for table load to complete.
    load_job.result()

def get_save_USD_BRL(request):
    tabela, tabelainfo = get_analytic_data()
    tablename = f"{tabelainfo['codein'][0]}-{tabelainfo['code'][0]}_{str(tabelainfo['date'][0])}"
    tablenameaux = f"Aux_{tabelainfo['codein'][0]}-{tabelainfo['code'][0]}_{str(tabelainfo['date'][0])}"
    save_bigquery(tabela, 'teste', tablename)
    save_bigquery(tabelainfo, 'teste', tablenameaux)

if __name__ == "__main__":
    start = datetime.now()
    print(f"Início: {start}")
    get_save_USD_BRL("request")
    end = datetime.now()
    print(f"Final: {end}")
    delta = end - start
    print(f"Diferença: {delta}")
    