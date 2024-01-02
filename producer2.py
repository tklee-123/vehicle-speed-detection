
from kafka import KafkaProducer
import threading
import matplotlib.pyplot as plt
import pymongo
from datetime import datetime
import json
import time
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["car_speed"]
lt20 = db["<20"]
f20t40 = db["20-40"]
f40t60 = db["40-60"]
f60t80 = db["60-80"]
f80t100 = db["80-100"]
gt100 = db[">100"]


def send_data_to_kafka(producer, topic):
    while True:

        # Assume you have speed data in different ranges
        lt20_count = db['<20'].count_documents({})
        f20t40_count = db['20-40'].count_documents({})
        f40t60_count = db['40-60'].count_documents({})
        f60t80_count = db['60-80'].count_documents({})
        f80t100_count = db['80-100'].count_documents({})
        gt100_count = db['>100'].count_documents({})

        data = {
            'lt20_count': lt20_count,
            'f20t40_count': f20t40_count,
            'f40t60_count': f40t60_count,
            'f60t80_count': f60t80_count,
            'f80t100_count': f80t100_count,
            'gt100_count': gt100_count,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send(topic, value=json.dumps(data).encode('utf-8'))
        time.sleep(1)
  


def main():
    producer = KafkaProducer(
        bootstrap_servers= ['localhost:9092'],
        api_version = (0,10,1)
    )
    
    topic3 = 'testc'


    producer_thread3 = threading.Thread(target=send_data_to_kafka, args=(producer, topic3))
    producer_thread3.start()
    producer_thread3.join()
    
if __name__ == "__main__":
    main()