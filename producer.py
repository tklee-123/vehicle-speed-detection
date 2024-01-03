import cv2
from kafka import KafkaProducer
import threading



def publish_video_to_kafka(producer, topic, video_path):
    cap = cv2.VideoCapture(video_path)
    
    while True:
        ret,frame = cap.read()
        if not ret:
            break
        
        _,buffer = cv2.imencode('.jpg', frame)
        data = buffer.tobytes()
        
        producer.send(topic, value= data)
        
    cap.release()
    


def main():
    producer = KafkaProducer(
        bootstrap_servers= ['localhost:9092'],
        api_version = (0,10,1)
    )
    
    topic1 = 'testa'
    topic4 = 'testd'
    video_path1 = 'files/videoTest.mp4'
    video_path2 = 'files/cars.mp4'
    
    producer_thread1 = threading.Thread(target=publish_video_to_kafka, args=(producer, topic1, video_path1))
    producer_thread4 = threading.Thread(target=publish_video_to_kafka, args=(producer, topic4, video_path2))
    
    producer_thread1.start()
    producer_thread4.start()
    
    producer_thread1.join()
    producer_thread4.join()
    
if __name__ == "__main__":
    main()