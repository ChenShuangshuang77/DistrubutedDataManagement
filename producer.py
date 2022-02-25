import sys
from kafka import KafkaProducer
import cv2 as cv

if __name__ == '__main__':
    cap = cv.VideoCapture('http://10.4.105.31/mjpg/video.mjpg')

    producer = KafkaProducer(bootstrap_servers='rt321vmw102.gtr.tp:6667')
    while cap.isOpend():
        ret,frame = cap.read()
        if ret:
            gray = cv.cvtColor(frame, cv.COLOR_BGR2GRAY)
            producer.send('gdm102s10', gray.tobytes())

    cap.release()
    cv.destroyAllWindows()