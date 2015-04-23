from boto.s3 import connect_to_region
from datetime import date
import gzip
import os

# Define AWS credentials, URLs, and bucket name
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
REGION = 'us-west-2'
DATABASE_URL = os.environ.get('DATABASE_URL')
BUCKET = 'landsat-pds'
NEW_SCENE_LIST_GZ = 'scene_list.gz'
OLD_SCENE_LIST = 'scene_list'

# Make connection to S3
S3conn = connect_to_region(REGION)

# Get landsat-pds bucket
landsat_pds_bucket = S3conn.get_bucket(BUCKET)

# Get new scene list file key
scene_list_key = landsat_pds_bucket.get_key(NEW_SCENE_LIST)

# Get new scene list file
new_scene_list_name = 'scene_list_{}'.format(date.today().strftime('%Y%m%d'))
scene_list_key.get_contents_to_filename('{}.gz'.format(new_scene_list_name))

# Load new scene list file
new_scene_list_file_object = gzip.open(new_scene_list_name)
new_scene_list = new_scene_list_file_object.readlines()

# Load old scene list file
old_scene_list_file_object = open(OLD_SCENE_LIST, 'rb')
old_scene_list = old_scene_list_file_object.readlines()

# Caluculate diffrence between old and new scene lists
diff = list(set(new_scene_list) - set(old_scene_list))

# Write diff to csv file
with open('diff.csv', 'wb') as d:
    d.writelines(diff)

# Close scene list files
new_scene_list_file_object.close()
old_scene_list_file_object.close()

# Remove old scene list file
os.remove(OLD_SCENE_LIST)

# Rename new scene list
os.rename(new_scene_list_name, OLD_SCENE_LIST)
