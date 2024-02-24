from vnstock import * 
import datetime
from key.keys import token
import os
import csv
from google.cloud import storage
currentdate = datetime.datetime.today().strftime("%Y-%m-%d")
oneyearpast = (datetime.datetime.today()- datetime.timedelta(days=360)).strftime("%Y-%m-%d") 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = rf"{token.dictionary_file()}/credentials.json"

bucket_name = "datastocks123"
source_file_name = f"{token.dictionary_file()}/oneyear.csv"
destination_file_name = "oneyear.csv"

class stock_storage():
    def stock_oneyear():
        list_stock = []
        with open(f"{token.dictionary_file()}/list_stock.txt","r") as file:
            stocks = file.read().split()
            count = len(stocks)
            for i in range(len(stocks)):
                try:
                    info_stock = stock_historical_data(symbol=stocks[i], start_date=oneyearpast, end_date=currentdate, resolution="1D", type="stock", beautify=True, decor=False, source='DNSE')
                    if i == 0:
                        info_stock.to_csv(f"{token.dictionary_file()}/oneyear.csv", index=False, header=True)
                    else:
                        info_stock.to_csv(f"{token.dictionary_file()}/oneyear.csv",mode='a', index=False, header=False)
                except:
                    pass
                print(f"{count-i}/{len(stocks)}: {stocks[i]}")
    def send_to_gcs():
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_file_name)
        blob.upload_from_filename(source_file_name)
        print('upload done')
        return True
    # stock_oneyear() 
    send_to_gcs()



