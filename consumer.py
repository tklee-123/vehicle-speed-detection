# import cv2
# from kafka import KafkaConsumer
# import threading
# import numpy as np
# from datetime import datetime
# import base64
# import dlib
# import time
# import pymongo

# WIDTH = 1280
# HEIGHT = 720
# cropBegin = 240
# mark1 = 120
# mark2 = 360
# markGap = 15
# fpsFactor = 3
# speedLimit = 20
# startTracker = {}
# endTracker = {}



# client = pymongo.MongoClient("mongodb://localhost:27017/")
# db = client["overspeeding_db"]
# collection = db["overspeeding_cars"]

# model_path = 'yolov4-tiny.weights'
# config_path = 'yolov4.cfg'
# classes_path = 'coco.names'

# net = cv2.dnn.readNet(model_path, config_path)
# classes = []
# with open(classes_path, 'r') as f:
#     classes = f.read().strip().split('\n')

# layer_names = net.getUnconnectedOutLayersNames()

# def blackout(image):
#     xBlack = 360
#     yBlack = 300
#     triangle_cnt = np.array( [[0,0], [xBlack,0], [0,yBlack]] )
#     triangle_cnt2 = np.array( [[WIDTH,0], [WIDTH-xBlack,0], [WIDTH,yBlack]] )
#     cv2.drawContours(image, [triangle_cnt], 0, (0,0,0), -1)
#     cv2.drawContours(image, [triangle_cnt2], 0, (0,0,0), -1)

#     return image

# # FUNCTION TO CALCULATE SPEED----------------------------------------------------
# def estimateSpeed(carID):
#     timeDiff = endTracker[carID] - startTracker[carID]
#     speed = round(markGap / timeDiff * fpsFactor * 3.6, 2)
#     print(f'Car {carID} - timeDiff: {timeDiff}, speed: {speed}')
#     return speed


# def saveCar(carID, speed, car_image):
#     now = datetime.today().now()
#     nameCurTime = now.strftime("%d-%m-%Y-%H-%M-%S-%f")
#     _, buffer = cv2.imencode('.jpg', car_image)
#     image_base64 = base64.b64encode(buffer).decode('utf-8')

#     # Prepare data to insert into MongoDB
#     car_data = {
#         "timestamp": nameCurTime,
#         "car_id": carID,
#         "speed": speed,
#         "image_base64": image_base64,
#     }

#     try:
#         global collection
#         collection.insert_one(car_data)
#         print("Car data saved successfully.")
#     except Exception as e:
#         print(f"Error saving car data: {e}")



# def detect_objects(frame):
#     blob = cv2.dnn.blobFromImage(frame, 0.00392, (416, 416), (0, 0, 0), True, crop=False)
#     net.setInput(blob)
#     outs = net.forward(layer_names)
#     return outs

# def process_frame(frame,currentCarID, carTracker):
#     outs = detect_objects(frame)
#     for out in outs:
#         for detection in out:
#             scores = detection[5:]
#             class_id = np.argmax(scores)
#             confidence = scores[class_id]
#             x, y, w, h = (np.array(detection[0:4]) * np.array([frame.shape[1], frame.shape[0], frame.shape[1], frame.shape[0]])).astype('int')
#             color = None
#             if confidence > 0.7 and classes[class_id] in ['car','bus']:
#                 if classes[class_id] == 'car':
#                     color = (0, 255, 0)  # Green for car
#                 elif classes[class_id] == 'bus':
#                     color = (128, 0, 128)  # Purple for bus
#                 cv2.rectangle(frame, (x - w//2, y - h//2), (x + w//2, y + h//2), color, 2)
#                 xbar = x + 0.5*w
#                 ybar = y + 0.5*h

#                 matchCarID = None

#                 #IF CENTROID OF CURRENT CAR NEAR THE CENTROID OF ANOTHER CAR IN PREVIOUS FRAME THEN THEY ARE THE SAME
#                 for carID in carTracker.keys():
#                     trackedPosition = carTracker[carID].get_position()
#                     tx = int(trackedPosition.left())
#                     ty = int(trackedPosition.top())
#                     tw = int(trackedPosition.width())
#                     th = int(trackedPosition.height())

#                     txbar = tx + 0.5 * tw
#                     tybar = ty + 0.5 * th

#                     if ((tx <= xbar <= (tx + tw)) and (ty <= ybar <= (ty + th)) and (x <= txbar <= (x + w)) and (y <= tybar <= (y + h))):
#                         matchCarID = carID

#                 if matchCarID is None:
#                     tracker = dlib.correlation_tracker()
#                     tracker.start_track(frame, dlib.rectangle(x, y, x + w, y + h))
#                     carTracker[currentCarID] = tracker
#                     currentCarID = currentCarID + 1


#     return frame,currentCarID,carTracker


# def consume_video(consumer, topic):
#     currentCarID = 0
#     carTracker = {}
#     for message in consumer:
#         frameTime = time.time()
#         frame_data = np.frombuffer(message.value, dtype=np.uint8)
#         frame = cv2.imdecode(frame_data, cv2.IMREAD_COLOR)
#         frame = cv2.resize(frame, (WIDTH, HEIGHT))[cropBegin:1400,0:2400]
#         frame = blackout(frame)
#         cv2.line(frame,(0,mark1),(1280,mark1),(0,0,255),2)
#         cv2.line(frame,(0,mark2),(1280,mark2),(0,0,255),2)
#         outs = detect_objects(frame)
#         for out in outs:
#             for detection in out:
#                 scores = detection[5:]
#                 class_id = np.argmax(scores)
#                 confidence = scores[class_id]
#                 x, y, w, h = (np.array(detection[0:4]) * np.array([frame.shape[1], frame.shape[0], frame.shape[1], frame.shape[0]])).astype('int')
#                 color = None
#                 if confidence > 0.7 and classes[class_id] in ['car','bus']:
#                     if classes[class_id] == 'car':
#                         color = (0, 255, 0)  # Green for car
#                     elif classes[class_id] == 'bus':
#                         color = (128, 0, 128)  # Purple for bus
#                     cv2.rectangle(frame, (x - w//2, y - h//2), (x + w//2, y + h//2), color, 2)
#                     xbar = x + 0.5*w
#                     ybar = y + 0.5*h

#                     matchCarID = None

#                     #IF CENTROID OF CURRENT CAR NEAR THE CENTROID OF ANOTHER CAR IN PREVIOUS FRAME THEN THEY ARE THE SAME
#                     for carID in carTracker.keys():
#                         trackedPosition = carTracker[carID].get_position()
#                         tx = int(trackedPosition.left())
#                         ty = int(trackedPosition.top())
#                         tw = int(trackedPosition.width())
#                         th = int(trackedPosition.height())

#                         txbar = tx + 0.5 * tw
#                         tybar = ty + 0.5 * th

#                         if ((tx <= xbar <= (tx + tw)) and (ty <= ybar <= (ty + th)) and (x <= txbar <= (x + w)) and (y <= tybar <= (y + h))):
#                             matchCarID = carID

#                     if matchCarID is None:
#                         tracker = dlib.correlation_tracker()
#                         tracker.start_track(frame, dlib.rectangle(x, y, x + w, y + h))
#                         carTracker[currentCarID] = tracker
#                         currentCarID = currentCarID + 1
#         for carID in carTracker.keys():
#             trackedPosition = carTracker[carID].get_position()
#             tx = int(trackedPosition.left())
#             ty = int(trackedPosition.top())
#             tw = int(trackedPosition.width())
#             th = int(trackedPosition.height())

#             #PUT BOUNDING BOXES-------------------------------------------------
            

#             # ESTIMATE SPEED-----------------------------------------------------
#             if carID not in startTracker and mark2 > ty+th > mark1 and ty < mark1:
#                 startTracker[carID] = frameTime
#                 print(f'Start tracking car {carID} at {frameTime}')
#             elif carID in startTracker and carID not in endTracker and mark2 < ty+th:
#                 endTracker[carID] = frameTime
#                 speed = estimateSpeed(carID)
#                 print(f'End tracking car {carID} at {frameTime}, speed: {speed}')
#                 if speed > speedLimit:
#                     print(f'CAR-ID: {carID} : {speed} kmph - OVERSPEED')
#                     saveCar(carID, speed, frame[ty:ty + th, tx:tx + tw])
#                 else:
#                     print(f'CAR-ID: {carID} : {speed} kmph')

#         #DISPLAY EACH FRAME
#         cv2.imshow('result', frame)

#         if cv2.waitKey(1) == ord('q'):
#             break

#     cv2.destroyAllWindows()

# # Run Kafka Consumer
# def main():
#     global collection

#     topic1 = "test"
    
#     consumer1 = KafkaConsumer(
#         topic1,
#         bootstrap_servers=["localhost:9092"],
#         api_version=(0, 10)
#     )
    
#     consumer_thread = threading.Thread(target=consume_video, args=(consumer1, topic1))
#     consumer_thread.start()
#     consumer_thread.join()

# if __name__ == "__main__":
#     main()







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
fpsFactor = 3 #TO COMPENSATE FOR SLOW PROCESSING
speedLimit = 20 #SPEEDLIMIT
startTracker = {} #STORE STARTING TIME OF CARS
endTracker = {} #STORE ENDING TIME OF CARS


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["overspeeding_db"]
collection = db["overspeeding_cars"]

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

def saveCar(carID, speed, car_image):
    now = datetime.today().now()
    nameCurTime = now.strftime("%d-%m-%Y-%H-%M-%S-%f")
    _, buffer = cv2.imencode('.jpg', car_image)
    image_base64 = base64.b64encode(buffer).decode('utf-8')

    # Prepare data to insert into MongoDB
    car_data = {
        "timestamp": nameCurTime,
        "car_id": carID,
        "speed": speed,
        "image_base64": image_base64,
    }

    try:
        global collection
        collection.insert_one(car_data)
        print("Car data saved successfully.")
    except Exception as e:
        print(f"Error saving car data: {e}")


def consume_video(consumer, topic):
    rectangleColor = (0, 255, 0)
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
        cv2.line(resultImage,(0,mark1),(1280,mark1),(0,0,255),2)
        cv2.line(resultImage,(0,mark2),(1280,mark2),(0,0,255),2)

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

            #PUT BOUNDING BOXES-------------------------------------------------
            cv2.rectangle(resultImage, (tx, ty), (tx + tw, ty + th), rectangleColor, 2)
            cv2.putText(resultImage, str(carID), (tx,ty-5), cv2.FONT_HERSHEY_DUPLEX, 1, (0, 255, 0), 1)

            #ESTIMATE SPEED-----------------------------------------------------
            if carID not in startTracker and mark2 > ty+th > mark1 and ty < mark1:
                startTracker[carID] = frameTime
                print(f'Start tracking car {carID} at {frameTime}')
            elif carID in startTracker and carID not in endTracker and mark2 < ty+th:
                endTracker[carID] = frameTime
                speed = estimateSpeed(carID)
                print(f'End tracking car {carID} at {frameTime}, speed: {speed}')
                if speed > speedLimit:
                    print(f'CAR-ID: {carID} : {speed} kmph - OVERSPEED')
                    saveCar(carID, speed, frame[ty:ty + th, tx:tx + tw])
                else:
                    print(f'CAR-ID: {carID} : {speed} kmph')


        #DISPLAY EACH FRAME
        cv2.imshow('result', resultImage)

        if cv2.waitKey(1) == ord('q'):
            break

    cv2.destroyAllWindows()

def main():
    topic1 = "test"
    
    consumer1 = KafkaConsumer(
        topic1,
        bootstrap_servers=["localhost:9092"],
        api_version=(0, 10)
    )
    
    consumer_thread = threading.Thread(target=consume_video,
                                       args=(consumer1, topic1))
    consumer_thread.start()
    consumer_thread.join()

if __name__ == "__main__":
    main()
