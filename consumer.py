import sys
import cv2 as cv
from kafka import KafkaConsumer

if __name__ == '__main__':
    confpm = SparkConf().setAppName('facedetect').setMaster('yarn')
    sc = SparkContext(conf=confpm)
    consumer = KafkaConsumer(bootstrap_servers='rt321vmw102.gtr.tp:6667')
    consumer.subscribe(['stream'])
    real_time = time.asctime(time.localtime(time.time()))

    images = []
    frame_limit = 10
    i = 0
    for msg in consumer:
        while i < frame_limit:
            images.append(msg.value)
            i+=1
        break

    classfier = cv.CascadeClassifier('haarcascade_frontalcatface_default.xml')
    faceRects = classfier.detectMultiScale(images, scaleFactor=1.2, minNeighbors=3, minSize=(32, 32))

    if len(faceRects) > 0:
        faceRects[:,2:] += faceRects[:,:2]
        x1,y1,x2,y2 = faceRects[0][0],faceRects[0][1],faceRects[0][2],faceRects[0][3]
    image = images[x1:x2,y1:y2]
    print ("Detect {0} faces".format(len(faceRects)))
    cv.imwrite('./face_'+str(real_time).replace(' ','')+'.png',imgage)