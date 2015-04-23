from boto.s3 import connect_to_region
from datetime import date
import gzip
import os
import psycopg2

# Define AWS credentials, URLs, and bucket name
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
DATABASE_URL = os.environ.get('DATABASE_URL')

REGION = 'us-west-2'
BUCKET = 'landsat-pds'
NEW_SCENE_LIST = 'scene_list.gz'
OLD_SCENE_LIST = 'scene_list'
DIFF = 'diff.csv'
TABLE = 'test'
SEP = ','


def main():
    # Make connection to S3
    S3conn = connect_to_region(REGION)

    # Get landsat-pds bucket
    landsat_pds_bucket = S3conn.get_bucket(BUCKET)

    # Get new scene list file key
    scene_list_key = landsat_pds_bucket.get_key(NEW_SCENE_LIST)

    # Get new scene list file
    new_scene_list_name = 'scene_list_{}'.format(date.today().strftime('%Y%m%d'))
    new_scene_list_name_gz = '{}.gz'.format(new_scene_list_name)
    scene_list_key.get_contents_to_filename(new_scene_list_name_gz)

    # Load new scene list file
    new_scene_list_file_object = gzip.open(new_scene_list_name_gz)
    new_scene_list = new_scene_list_file_object.readlines()

    # Load old scene list file
    old_scene_list_file_object = open(OLD_SCENE_LIST, 'rb')
    old_scene_list = old_scene_list_file_object.readlines()

    # Calculate diffrence between old and new scene lists
    diff = list(set(new_scene_list) - set(old_scene_list))

    # Write diff to csv file
    with open(DIFF, 'wb') as dif:
        dif.writelines(diff)

    # Write diff to db
    with open(DIFF, 'rb') as dif:
        # Connect to DB
        conn = psycopg2.connect(DATABASE_URL)

        # Create DB cursor
        cur = conn.cursor()

        # Copy diff to DB
        cur.copy_from(dif, TABLE, sep=SEP)

        # Commit changes
        conn.commit()

        # Close communication with the database
        cur.close()
        conn.close()

    # Close scene list files
    new_scene_list_file_object.close()
    old_scene_list_file_object.close()

    # Overwrite scene list file with new data
    with open(OLD_SCENE_LIST, 'wb') as old:
        old.writelines(new_scene_list)

    # Remove new scene list file
    os.remove(new_scene_list_name_gz)

if __name__ == '__main__':
    main()
