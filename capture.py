import cv2 as cv
from pyspark import SparkConf, SparkContext
import time
if __name__ == '__main__':
    print('-----------START DETECTED-------------------')
    cam = cv.VideoCapture('http://10.4.105.31/mjpg/video.mjpg')
    confpm = SparkConf().setAppName('facedetect').setMaster('yarn')
    sc = SparkContext(conf=confpm)
    localtime = time.asctime(time.localtime(time.time()))
    while True:
        ret,frame = cam.read()
        img = frame
        gray = cv.cvtColor(img,cv.COLOR_BGR2GRAY)
        face_cascade = cv.CascadeClassifier('haarcascade_frontalcatface_default.xml')
        faces = face_cascade.detectMultiScale(images, scaleFactor=1.2, minNeighbors=3, minSize=(32, 32))
        if len(faces) > 0:
            faces[:,2:] += faces[:,:2]
            x1,y1,x2,y2 = faces[0][0],faces[0][1],faces[0][2],faces[0][3]
        img = img[x1:x2,y1:y2]
        print ("Detect {0} faces".format(len(faces)))
        cv.imwrite('./face_'+str(localtime).replace(' ','')+'.png',img)
        break
    else:
        print('-----------NO FACE DETECTED----------')