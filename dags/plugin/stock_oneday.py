from vnstock import * 
import datetime
from plugin.key.keys import token
import os
import csv
from google.cloud import storage
import datetime
currentdate = datetime.datetime.today().strftime("%Y-%m-%d")
# onedaypast = (datetime.datetime.today()- datetime.timedelta(days=1)).strftime("%Y-%m-%d") 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = rf"{token.dictionary_file()}/credentials.json"
bucket_name = "datastocks123"
source_file_name = f"{token.dictionary_file()}/daily.csv"
destination_file_name = f"daily/daily_{currentdate}.csv"


class stock_storage_oneday():
    def stock_oneday():
        with open(f"{token.dictionary_file()}/list_stock.txt","r") as file:
            stocks = file.read().split()
            count = len(stocks)
            for i in range(len(stocks)):
                try:
                    info_stock = stock_historical_data(symbol=stocks[i], start_date=currentdate, end_date=currentdate, resolution="1H", type="stock", beautify=True, decor=False, source='DNSE')
                    if i == 0:
                        info_stock.to_csv(f"{token.dictionary_file()}/daily.csv", index=False, header=True)
                    else:
                        info_stock.to_csv(f"{token.dictionary_file()}/daily.csv",mode='a', index=False, header=False)
                except:
                    pass
                print(info_stock)
                print(f"{count-i}/{len(stocks)}: {stocks[i]}")


    def send_to_gcs():
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_file_name)
        blob.upload_from_filename(source_file_name)
        print('upload done')
        return True

