
import cv2
from kafka import KafkaConsumer
import threading
import numpy as np
from datetime import datetime
import base64
import dlib
import time
import pymongo

WIDTH = 1280 #WIDTH OF VIDEO FRAME
HEIGHT = 720 #HEIGHT OF VIDEO FRAME
cropBegin = 200 #CROP VIDEO FRAME FROM THIS POINT
mark1 = 120 #MARK TO START TIMER
mark2 = 360 #MARK TO END TIMER
markGap = 15 #DISTANCE IN METRES BETWEEN THE MARKERS
fpsFactor = 10 #TO COMPENSATE FOR SLOW PROCESSING
startTracker = {} #STORE STARTING TIME OF CARS
endTracker = {} #STORE ENDING TIME OF CARS

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["car_speed"]
lt20 = db["<20"]
f20t40 = db["20-40"]
f40t60 = db["40-60"]
f60t80 = db["60-80"]
f80t100 = db["80-100"]
gt100 = db[">100"]

def blackout(image):
    xBlack = 360
    yBlack = 300
    triangle_cnt = np.array( [[0,0], [xBlack,0], [0,yBlack]] )
    triangle_cnt2 = np.array( [[WIDTH,0], [WIDTH-xBlack,0], [WIDTH,yBlack]] )
    cv2.drawContours(image, [triangle_cnt], 0, (0,0,0), -1)
    cv2.drawContours(image, [triangle_cnt2], 0, (0,0,0), -1)

    return image
def estimateSpeed(carID):
    timeDiff = endTracker[carID]-startTracker[carID]
    speed = round(markGap/timeDiff*fpsFactor*3.6,2)
    return speed
carCascade = cv2.CascadeClassifier('files/HaarCascadeClassifier.xml')

def saveCar(speed, car_image, topic):
    now = datetime.today().now()
    nameCurTime = now.strftime("%d-%m-%Y-%H-%M-%S-%f")
    _, buffer = cv2.imencode('.jpg', car_image)
    image_base64 = base64.b64encode(buffer).decode('utf-8')

    # Prepare data to insert into MongoDB
    car_data = {
        "timestamp": nameCurTime,
        "speed": speed,
        "image_base64": image_base64,
        "land": topic
    }

    try:
        global lt20,f20t40,f40t60,f60t80,f80t100,gt100
        if speed > 0 and speed <= 20:
            lt20.insert_one(car_data)
        elif speed <= 40:
            f20t40.insert_one(car_data)
        elif speed <= 60: 
            f40t60.insert_one(car_data)
        elif speed <= 80: 
            f60t80.insert_one(car_data)
        elif speed <= 100: 
            f80t100.insert_one(car_data)
        else:
            gt100.insert_one(car_data)
        print("Car data saved successfully.")
    except Exception as e:
        print(f"Error saving car data: {e}")

def render_origin_video1(consumer,topic):
    for message in consumer:
        fram_data = np.frombuffer(message.value, dtype=np.uint8)
        frame = cv2.imdecode(fram_data, cv2.IMREAD_COLOR)
        image = cv2.resize(frame, (WIDTH, HEIGHT))[cropBegin:750:,0:2400]
        resultImage = blackout(image)
        cv2.imshow('result1', resultImage)

        if cv2.waitKey(1) == ord('q'):
            break
def render_origin_video2(consumer,topic):
    for message in consumer:
        fram_data = np.frombuffer(message.value, dtype=np.uint8)
        frame = cv2.imdecode(fram_data, cv2.IMREAD_COLOR)
        image = cv2.resize(frame, (WIDTH, HEIGHT))[cropBegin:750:,0:2400]
        resultImage = blackout(image)
        cv2.imshow('result2', resultImage)

        if cv2.waitKey(1) == ord('q'):
            break

    cv2.destroyAllWindows()
    
def consume_video1(consumer, topic):
    frameCounter = 0
    currentCarID = 0
    carTracker = {}
    for message in consumer:
        fram_data = np.frombuffer(message.value, dtype=np.uint8)
        frame = cv2.imdecode(fram_data, cv2.IMREAD_COLOR)
        image = cv2.resize(frame, (WIDTH, HEIGHT))[cropBegin:750:,0:2400]
        frameTime = time.time()
        # Detect objects in the frame
        resultImage = blackout(image)

        frameCounter = frameCounter + 1

        #DELETE CARIDs NOT IN FRAME---------------------------------------------
        carIDtoDelete = []
        for carID in carTracker.keys():
            trackingQuality = carTracker[carID].update(image)

            if trackingQuality < 7:
                carIDtoDelete.append(carID)

        for carID in carIDtoDelete:
            carTracker.pop(carID, None)

        #MAIN PROGRAM-----------------------------------------------------------
        if (frameCounter%60 == 0):
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            cars = carCascade.detectMultiScale(gray, 1.1, 15, 20, (30, 30)) #DETECT CARS IN FRAME

            for (_x, _y, _w, _h) in cars:
                #GET POSITION OF A CAR
                x = int(_x)
                y = int(_y)
                w = int(_w)
                h = int(_h)

                xbar = x + 0.5*w
                ybar = y + 0.5*h

                matchCarID = None

                #IF CENTROID OF CURRENT CAR NEAR THE CENTROID OF ANOTHER CAR IN PREVIOUS FRAME THEN THEY ARE THE SAME
                for carID in carTracker.keys():
                    trackedPosition = carTracker[carID].get_position()

                    tx = int(trackedPosition.left())
                    ty = int(trackedPosition.top())
                    tw = int(trackedPosition.width())
                    th = int(trackedPosition.height())

                    txbar = tx + 0.5 * tw
                    tybar = ty + 0.5 * th

                    if ((tx <= xbar <= (tx + tw)) and (ty <= ybar <= (ty + th)) and (x <= txbar <= (x + w)) and (y <= tybar <= (y + h))):
                        matchCarID = carID


                if matchCarID is None:
                    tracker = dlib.correlation_tracker()
                    tracker.start_track(image, dlib.rectangle(x, y, x + w, y + h))

                    carTracker[currentCarID] = tracker

                    currentCarID = currentCarID + 1


        for carID in carTracker.keys():
            trackedPosition = carTracker[carID].get_position()

            tx = int(trackedPosition.left())
            ty = int(trackedPosition.top())
            tw = int(trackedPosition.width())
            th = int(trackedPosition.height())

            #ESTIMATE SPEED-----------------------------------------------------
            if carID not in startTracker and mark2 > ty+th > mark1 and ty < mark1:
                startTracker[carID] = frameTime
                print(f'Start tracking car {carID} at {frameTime}')
            elif carID in startTracker and carID not in endTracker and mark2 < ty+th:
                endTracker[carID] = frameTime
                speed = estimateSpeed(carID)
                print(f'End tracking car {carID} at {frameTime}, speed: {speed}')
                saveCar(speed, frame[ty:ty + th, tx:tx + tw],topic)
                

def consume_video2(consumer, topic):
    frameCounter = 0
    currentCarID = 0
    carTracker = {}
    for message in consumer:
        fram_data = np.frombuffer(message.value, dtype=np.uint8)
        frame = cv2.imdecode(fram_data, cv2.IMREAD_COLOR)
        image = cv2.resize(frame, (WIDTH, HEIGHT))[cropBegin:750:,0:2400]
        frameTime = time.time()
        # Detect objects in the frame
        resultImage = blackout(image)

        frameCounter = frameCounter + 1

        #DELETE CARIDs NOT IN FRAME---------------------------------------------
        carIDtoDelete = []
        for carID in carTracker.keys():
            trackingQuality = carTracker[carID].update(image)

            if trackingQuality < 7:
                carIDtoDelete.append(carID)

        for carID in carIDtoDelete:
            carTracker.pop(carID, None)

        #MAIN PROGRAM-----------------------------------------------------------
        if (frameCounter%60 == 0):
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            cars = carCascade.detectMultiScale(gray, 1.1, 15, 20, (30, 30)) #DETECT CARS IN FRAME

            for (_x, _y, _w, _h) in cars:
                #GET POSITION OF A CAR
                x = int(_x)
                y = int(_y)
                w = int(_w)
                h = int(_h)

                xbar = x + 0.5*w
                ybar = y + 0.5*h

                matchCarID = None

                #IF CENTROID OF CURRENT CAR NEAR THE CENTROID OF ANOTHER CAR IN PREVIOUS FRAME THEN THEY ARE THE SAME
                for carID in carTracker.keys():
                    trackedPosition = carTracker[carID].get_position()

                    tx = int(trackedPosition.left())
                    ty = int(trackedPosition.top())
                    tw = int(trackedPosition.width())
                    th = int(trackedPosition.height())

                    txbar = tx + 0.5 * tw
                    tybar = ty + 0.5 * th

                    if ((tx <= xbar <= (tx + tw)) and (ty <= ybar <= (ty + th)) and (x <= txbar <= (x + w)) and (y <= tybar <= (y + h))):
                        matchCarID = carID


                if matchCarID is None:
                    tracker = dlib.correlation_tracker()
                    tracker.start_track(image, dlib.rectangle(x, y, x + w, y + h))

                    carTracker[currentCarID] = tracker

                    currentCarID = currentCarID + 1


        for carID in carTracker.keys():
            trackedPosition = carTracker[carID].get_position()

            tx = int(trackedPosition.left())
            ty = int(trackedPosition.top())
            tw = int(trackedPosition.width())
            th = int(trackedPosition.height())

            #ESTIMATE SPEED-----------------------------------------------------
            if carID not in startTracker and mark2 > ty+th > mark1 and ty < mark1:
                startTracker[carID] = frameTime
                print(f'Start tracking car {carID} at {frameTime}')
            elif carID in startTracker and carID not in endTracker and mark2 < ty+th:
                endTracker[carID] = frameTime
                speed = estimateSpeed(carID)
                print(f'End tracking car {carID} at {frameTime}, speed: {speed}')
                saveCar(speed, frame[ty:ty + th, tx:tx + tw],topic)
                

def main():
    topic1 = "testa"
    topic2 = 'testb'
    topic4 = 'testd'
    consumer1 = KafkaConsumer(
        topic1,
        bootstrap_servers=["localhost:9092"],
        api_version=(0, 10)
    )
    consumer2 = KafkaConsumer(
        topic1,
        bootstrap_servers=["localhost:9092"],
        api_version=(0, 10)
    )
    consumer4 = KafkaConsumer(
        topic4,
        bootstrap_servers=["localhost:9092"],
        api_version=(0, 10)
    )
    consumer5 = KafkaConsumer(
        topic4,
        bootstrap_servers=["localhost:9092"],
        api_version=(0, 10)
    )
    
    consumer_thread1 = threading.Thread(target=render_origin_video1,
                                       args=(consumer1, topic1))
    
    consumer_thread4 = threading.Thread(target=render_origin_video2,
                                       args=(consumer4, topic4))
    
    consumer_thread2 = threading.Thread(target=consume_video1,
                                       args=(consumer2, topic1))
    consumer_thread5 = threading.Thread(target=consume_video2,
                                       args=(consumer5, topic4))
    
    consumer_thread1.start()
    consumer_thread4.start()
    consumer_thread2.start()
    consumer_thread5.start()

    consumer_thread1.join()
    consumer_thread4.join()
    consumer_thread2.join()
    consumer_thread5.join()

if __name__ == "__main__":
    main()
