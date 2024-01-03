import cv2
from kafka import KafkaConsumer
import threading
import numpy as np
from datetime import datetime
import base64
import dlib
import time
import pymongo

WIDTH = 1280
HEIGHT = 720
cropBegin = 240
mark1 = 120
mark2 = 360
markGap = 15
fpsFactor = 3
speedLimit = 20
startTracker = {}
endTracker = {}

model_path = 'yolov4-tiny.weights'
config_path = 'yolov4.cfg'
classes_path = 'coco.names'

net = cv2.dnn.readNet(model_path, config_path)
classes = []
with open(classes_path, 'r') as f:
    classes = f.read().strip().split('\n')
layer_names = net.getUnconnectedOutLayersNames()

def detect_objects(frame):
    blob = cv2.dnn.blobFromImage(frame, 0.00392, (416, 416), (0, 0, 0), True, crop=False)
    net.setInput(blob)
    outs = net.forward(layer_names)
    return outs

def blackout(image):
    xBlack = 360
    yBlack = 300
    triangle_cnt = np.array( [[0,0], [xBlack,0], [0,yBlack]] )
    triangle_cnt2 = np.array( [[WIDTH,0], [WIDTH-xBlack,0], [WIDTH,yBlack]] )
    cv2.drawContours(image, [triangle_cnt], 0, (0,0,0), -1)
    cv2.drawContours(image, [triangle_cnt2], 0, (0,0,0), -1)

    return image

def consume_video(consumer, topic):
    for message in consumer:
        frame_data = np.frombuffer(message.value, dtype=np.uint8)
        frame = cv2.imdecode(frame_data, cv2.IMREAD_COLOR)
        frame = cv2.resize(frame, (WIDTH, HEIGHT))[cropBegin:1400,0:2400]
        frame = blackout(frame)
        cv2.line(frame,(0,mark1),(1280,mark1),(0,0,255),2)
        cv2.line(frame,(0,mark2),(1280,mark2),(0,0,255),2)
        outs = detect_objects(frame)
        for out in outs:
            for detection in out:
                        scores = detection[5:]
                        class_id = np.argmax(scores)
                        confidence = scores[class_id]
                        x, y, w, h = (np.array(detection[0:4]) * np.array([frame.shape[1], frame.shape[0], frame.shape[1], frame.shape[0]])).astype('int')
                        color = None
                        if confidence > 0.7 and classes[class_id] in ['car','bus']:
                            if classes[class_id] == 'car':
                                color = (0, 255, 0)  # Green for car
                            elif classes[class_id] == 'bus':
                                color = (128, 0, 128)  # Purple for bus
                            cv2.rectangle(frame, (x - w//2, y - h//2), (x + w//2, y + h//2), color, 2)
                            
                            