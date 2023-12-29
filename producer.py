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
    
    topic1 = 'test'
    video_path1 = 'files/videoTest.mp4'
    
    producer_thread = threading.Thread(target=publish_video_to_kafka, args=(producer, topic1, video_path1))
    producer_thread.start()

    producer_thread.join()
    
if __name__ == "__main__":
    main()