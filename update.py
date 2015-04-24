from boto.s3 import connect_to_region
from datetime import date
import gzip
import os
import psycopg2

# Constants
CREDENTIALS = 'credentials'
REGION = 'us-west-2'
BUCKET = 'landsat-pds'
NEW_SCENE_LIST = 'scene_list.gz'
OLD_SCENE_LIST = 'scene_list'
DIFF = 'diff.csv'
TABLE = 'path_row'
SEP = ','


def diff_to_db(DATABASE_URL, dif):
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


def write_to_update_log():
    pass


def main():
    # Get current directory
    file_path = os.path.dirname(os.path.realpath(__file__))

    # Get credentials
    with open(file_path + '/' + CREDENTIALS, 'rb') as cred:
        creds = cred.readlines()

    # Assign credentials
    AWS_ACCESS_KEY_ID = creds[0].rstrip('\n')
    AWS_SECRET_ACCESS_KEY = creds[1].rstrip('\n')
    DATABASE_URL = creds[2].rstrip('\n')

    # Make connection to S3
    S3conn = connect_to_region(REGION,
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Get landsat-pds bucket
    landsat_pds_bucket = S3conn.get_bucket(BUCKET)

    # Get new scene list file key
    scene_list_key = landsat_pds_bucket.get_key(NEW_SCENE_LIST)

    # Get new scene list file
    new_scene_list_name = 'scene_list_{}'.format(date.today().strftime('%Y%m%d'))
    new_scene_list_name_gz = '{}.gz'.format(new_scene_list_name)
    scene_list_key.get_contents_to_filename(new_scene_list_name_gz)

    with gzip.open(new_scene_list_name_gz) as new_scene_list_file_object, \
            open(file_path + '/' + OLD_SCENE_LIST, 'rb') as old_scene_list_file_object:

        # Load new scene list file
        new_scene_list = new_scene_list_file_object.readlines()

        # Load old scene list file
        old_scene_list = old_scene_list_file_object.readlines()

        # Calculate diffrence between old and new scene lists
        diff = list(set(new_scene_list) - set(old_scene_list))

    # Write diff to csv file
    with open(file_path + '/' + DIFF, 'wb') as dif:
        dif.writelines(diff)

    # Write diff to db
    with open(file_path + '/' + DIFF, 'rb') as dif:
        diff_to_db(DATABASE_URL, dif)

    # Overwrite scene list file with new data
    with open(file_path + '/' + OLD_SCENE_LIST, 'wb') as old:
        old.writelines(new_scene_list)

    # Remove new scene list file
    os.remove(new_scene_list_name_gz)

if __name__ == '__main__':
    main()
