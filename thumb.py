__author__='paul'
import os
import sys
from time import time as _time
from cStringIO import StringIO
#from contextlib import closing
from PIL import ImageOps,Image
from threading import Thread
from Queue import Queue
from txaws.s3.client import S3Client
from txaws.credentials import AWSCredentials
from twisted.web import server, resource
from twisted.web.client import Agent
from twisted.internet import reactor
from twisted.python.logfile import DailyLogFile
from twisted.python import log
from twisted.python.failure import Failure
from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
import settings

log.startLogging(DailyLogFile.fromFullPath("logs/thumb.log"))

agent= Agent(reactor)

with open("blank.png") as blank:
    blank_image_data= blank.read()
blank_image_content_type= "png"

class FakeRequest(object):
    def __getattribute__(self,item):
        return lambda *args,**kwargs: None

def make_conn(f):
    def wrapped(*args,**kwargs):
        if not kwargs.get("conn",None):
            kwargs["conn"]= S3Connection(settings.ACCESS_KEY,settings.SECRET_KEY)
        return f(*args,**kwargs)
    return wrapped

def time_it(f):
    def wrapped(*args,**kwargs):
        start_time= _time()
        return_val= f(*args,**kwargs)
        print f.__name__,_time()-start_time
        return return_val
    return wrapped

def getFileObjectLen(file_object):
    current= file_object.tell()
    file_object.seek(0,os.SEEK_END)
    length= file_object.tell()
    file_object.seek(current)
    return length

class StringIOWrapper(object):
    def __init__(self,stringio_obj):
        self.stringio_obj= stringio_obj

    def __getattribute__(self, item):
        try:
            return object.__getattribute__(self,item)
        except AttributeError:
            return getattr(object.__getattribute__(self,"stringio_obj"),item)

    def __len__(self):
        return getFileObjectLen(self.stringio_obj)

class WorkerThread(Thread):
    def __init__(self,queue):
        Thread.__init__(self)
        self.conn= S3Connection(settings.ACCESS_KEY,settings.SECRET_KEY)
        self.thumb_bucket= Bucket(self.conn,settings.THUMB_BUCKET)
        self.store_bucket= Bucket(self.conn,settings.STORE_BUCKET)
        self.buckets= {settings.THUMB_BUCKET: self.thumb_bucket,
                       settings.STORE_BUCKET: self.store_bucket}
        self.queue= queue
        self.daemon= True
        self.start()

    def calculateSize(self,width,height,image_size,maintain):
        """
        If either width or height are 0, it is set to match the original images'
        aspect ratio.
        """
        if maintain and not all((width,height)):
            image_width,image_height= image_size
            if width==0:
                ratio= image_width/float(image_height)
                width= int(ratio*height)
            elif height==0:
                ratio= image_height/float(image_width)
                height= int(ratio*width)
            maintain= False
        width= max(1,width)
        height= max(1,height)
        return width,height,maintain

#    @time_it
    def generateThumb(self,image,width,height,maintain,sample_type):
        """Generates a thumbnail from an image with the specified width and height"""
        try:
            if maintain:
                new_image= ImageOps.fit(image,(width,height),sample_type)
            else:
    #            image.thumbnail((width,height),sample_type)
                new_image= image.resize((width,height),sample_type)
        finally:
            del image
        return new_image

    def reduce(self,image,width,height,sample_type):
        """
        Scales down an image to a square so that it can be rotated and still
        keep enough of the image to be thumnailed
        """
        largest_dim= max(width,height)
        square_size= (largest_dim,largest_dim)
        reduced_image= ImageOps.fit(image,square_size,sample_type)
        del image
        return reduced_image

    def rotateImage(self,image,exif,width,height,sample_type):
        """Rotates images based on the exif data"""
#        if hasattr(image,"_getexif"):
#            exif= image._getexif()
        if exif is not None:
            orientation= exif.get(settings.ORIENTATION_TAG,None)
            if orientation == 3:
                #Reduce the image first to lower the cost of the rotate
                image= self.reduce(image,width,height,sample_type)
                new_image= image.transpose(Image.ROTATE_180)
                del image
                image= new_image
            elif orientation == 6:
                image= self.reduce(image,width,height,sample_type)
                new_image= image.transpose(Image.ROTATE_270)
                del image
                image= new_image
            elif orientation == 8:
                image= self.reduce(image,width,height,sample_type)
                new_image= image.transpose(Image.ROTATE_90)
                del image
                image= new_image
        return image

    def getExifData(self,image):
        """Gets any exif data from an image if it has any, else returns None"""
        if hasattr(image,"_getexif"):
            return image._getexif()

    def getBlankImage(self,thumb_data):
        """Returns a 1x1 transparent png"""
        thumb_data.write(blank_image_data)
        thumb_data.seek(0)
        return thumb_data,blank_image_content_type,True

#    @time_it
    def doThumbGenerate(self,image_data,thumb_data,image_key,width,height,maintain,sample_type):
#        image_data= self.downloadImage(image_key,settings.STORE_BUCKET)
        key= self.store_bucket.get_key(image_key)
#        with closing(StringIO(key.get_contents_as_string())) as image_data:
        key.get_contents_to_file(image_data)
        image_data.seek(0)
        try:
            image= Image.open(image_data)
        except IOError:
            return self.getBlankImage(thumb_data)
        try:
            image_format= image.format
            width,height,maintain= self.calculateSize(width,height,image.size,maintain)
            exif_data= self.getExifData(image)
            try:
                image= self.rotateImage(image,exif_data,width,height,sample_type)
                thumb_image= self.generateThumb(image,width,height,maintain,sample_type)
            except IOError:
                return self.getBlankImage(thumb_data)
        finally:
            del image
        try:
            thumb_format= settings.CONVERSIONS.get(image_format,image_format)
            thumb_image.save(thumb_data,thumb_format)
            thumb_data.seek(0)
        finally:
            del thumb_image
        return thumb_data,thumb_format,False

#    @time_it
    def runTask(self,func,args,kwargs):
        if func==settings.TASK_GENERATE_THUMB:
            return self.doThumbGenerate(*args,**kwargs)
        else:
            raise KeyError("Received unexpected task: %s"%repr(func))

    def run(self):
        while True:
            priority,work,callback,errback= self.queue.get()
            try:
                func,args,kwargs= work
                try:
                    response= self.runTask(func,args,kwargs)
                except Exception as err:
                    reactor.callLater(0,errback,Failure())
                else:
                    if not isinstance(response,tuple):
                        response= (response,)
                    reactor.callLater(0,callback,*response)
            except Exception as err:
                log.err(Failure())
            finally:
                self.queue.task_done()

class Thumb(resource.Resource):
    isLeaf = True
    def __init__(self):
        resource.Resource.__init__(self)
        self.queue= Queue()
        self.boto_conn= S3Connection(settings.ACCESS_KEY,settings.SECRET_KEY)
        self.tx_conn= S3Client(AWSCredentials(settings.ACCESS_KEY,settings.SECRET_KEY))
        for _ in xrange(settings.THREAD_COUNT):
            WorkerThread(self.queue)

#    @time_it
    def verifyRequest(self,verify,checksum,sighash,uid,width,height):
        """Only process requests with the correct verify hash"""
        return True #removed actual verify code

#    @time_it
    def generate_key_url(self,bucket_name,key_name):
        return self.boto_conn.generate_url(settings.TEN_DAYS,"GET",bucket_name,key_name,force_http=True)

    def formatArgument(self,arg):
        return str(arg).lower()=="true" or str(arg)=="1"

    def singleRequest(self,request):
        """
        Accepts single thumbnail generation requests with the uri in the format;
        'checksum;sighash;verify_hash;user_id;thumb_width;thumb_height;maintain_aspect_ratio[;anti_aliased_output]'
        """
        if len(request.uri) < settings.REQUEST_MAX_CHARS:
            args= request.uri[1:].split(";")[:8]
            if len(args)==7:
                args.append(False)
            if len(args)==8:
                checksum,sighash,verify,uid,width,height,maintain,anti_alias=args
                width= max(int(width),0)
                height= max(int(height),0)
                if width or height:
                    if self.verifyRequest(verify,checksum,sighash,uid,width,height):
                        maintain= self.formatArgument(maintain)
                        anti_alias= self.formatArgument(anti_alias)
                        image_key= "%s.%s"%(checksum,sighash)
                        self.doWork(request,image_key,width,height,maintain,anti_alias)
                        return server.NOT_DONE_YET
        return ""

    def putWork(self,task,callback,errback,time_received,targs=(),tkwargs={}):
        """Put work into the thread pool queue, with callback and errback"""
        priority= time_received
        work= task,targs,tkwargs
        self.queue.put((priority,work,callback,errback),block=False)

    def doWork(self,request,image_key,width,height,maintain,anti_alias):
        """Process a request"""
        time_received= 0
        thumb_key= "%s%s%s.%s-%s"%(image_key,".m" if maintain else "",".a" if anti_alias else "",width,height)
        image_data= StringIO()
        thumb_data= StringIO()
        def redirect(ignored=None):
            request.redirect(thumb_key_url)
            success()
        def check_fork(response):
#            broken= response.headers.hasHeader("x-amz-meta-broken")
            if response.code / 100 == 2:
                redirect()
            else:
#                d= self.tx_conn.get_object(settings.STORE_BUCKET,image_key)
#                d.addCallback(generate_thumb)
#                d.addErrback(error)
#        def generate_thumb(image_data):
                sample_type= Image.ANTIALIAS if anti_alias else Image.NEAREST
                self.putWork(settings.TASK_GENERATE_THUMB,return_thumb,error,time_received,
                             targs=(image_data,thumb_data,image_key,width,height,maintain,sample_type))
        def return_thumb(thumb_data,thumb_format,broken):
            metadata= {}
            if broken:
                metadata["broken"]= True
            d= self.tx_conn.put_object(settings.THUMB_BUCKET,thumb_key,thumb_data.getvalue(),
                                       ("image/%s"%thumb_format).lower(),metadata=metadata,
                                       amz_headers={"storage-class":"REDUCED_REDUNDANCY"})
            d.addCallback(redirect)
            d.addErrback(error)
        def success():
            clean_up()
            if not request._disconnected:
                request.finish()
        def error(failure):
            clean_up()
            log.err(failure)
            if not request._disconnected:
                request.finish()
        def clean_up():
            image_data.close()
            thumb_data.close()
        thumb_key_url= self.generate_key_url(settings.THUMB_BUCKET,thumb_key)
        d= agent.request("GET",thumb_key_url)
        d.addCallback(check_fork)
        d.addErrback(error)

    def render_GET(self, request):
        return self.singleRequest(request)

if __name__ == "__main__":
    site = server.Site(Thumb())
    reactor.listenTCP(80, site)
    reactor.run()
