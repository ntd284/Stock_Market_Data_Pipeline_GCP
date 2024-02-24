from vnstock import * 
import pandas
from key.keys import token
import datetime
from google.cloud import pubsub_v1
current_date = datetime.datetime.today().strftime("%Y-%m-%d")
credentials_path = f'{token.dictionary_file()}/credentials.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credentials_path

publisher = pubsub_v1.PublisherClient()
topic_path= 'projects/just-shell-415015/topics/mod_stock'

class stock_subscribe():
    def get_data_realtime():
        with open(f"{token.dictionary_file()}/stock_code.txt","r") as file:
            list_stock = file.read().split()
            for stock in list_stock:
                data_lastest = stock_intraday_data(symbol=stock, page_size=1, investor_segment=False)
                df_data = data_lastest.to_dict(orient='records')
                json_data = {
                    "ticker" : df_data[0]['ticker'],
                    "time" : f"{current_date} {df_data[0]['time']}",
                    "price" : df_data[0]['price'],
                }
                print(json_data)
                bytestring = str(json_data).replace("'",'"').encode('utf-8')
                future = publisher.publish(topic_path,bytestring)
                print(f'publish message id {future.result()}')
            
    get_data_realtime()
    
        
