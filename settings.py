__author__='paul'
import ExifTags, Image

for ORIENTATION_TAG,_descriptor in ExifTags.TAGS.iteritems():
    if _descriptor=="Orientation":
        break
else:
    ORIENTATION_TAG= 0x0112

ACCESS_KEY= "S3_ACCESS_KEY"
SECRET_KEY= "S3_SECRET_KEY"

STORE_BUCKET= "NAME_OF_STORE_BUCKET"
THUMB_BUCKET= "NAME_OF_THUMB_BUCKET"

CONVERSIONS= {"TIFF":"PNG"}

TEN_DAYS= 60*60*24*10

REQUEST_MAX_CHARS= 300

TASK_DOES_THUMB_KEY_EXIST= 1
TASK_GENERATE_THUMB= 2
TASK_UPLOAD_THUMB= 3

THREAD_COUNT= 60
