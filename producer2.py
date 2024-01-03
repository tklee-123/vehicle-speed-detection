
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
        # Assume you have a collection named 'speed_data' with documents having 'land' and 'speed' fields
        pipeline = [
            {"$group": {"_id": {"land": "$land", "speed": "$speed"}, "count": {"$sum": 1}}}
        ]

        cursor = db.speed_data.aggregate(pipeline)

        # Initialize counts for each speed range and land
        speed_range_counts = {"<20": 0, "20-40": 0, "40-60": 0, "60-80": 0, "80-100": 0, ">100": 0}

        for document in cursor:
            land = document["_id"]["land"]
            speed = document["_id"]["speed"]
            count = document["count"]

            # Update counts based on speed range
            if speed < 20:
                speed_range_counts["<20"] += count
            elif 20 <= speed < 40:
                speed_range_counts["20-40"] += count
            elif 40 <= speed < 60:
                speed_range_counts["40-60"] += count
            elif 60 <= speed < 80:
                speed_range_counts["60-80"] += count
            elif 80 <= speed < 100:
                speed_range_counts["80-100"] += count
            else:
                speed_range_counts[">100"] += count

        # Add timestamp
        speed_range_counts['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        producer.send(topic, value=json.dumps(speed_range_counts).encode('utf-8'))
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