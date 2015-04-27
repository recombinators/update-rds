from boto.s3 import connect_to_region
from datetime import date, datetime
import gzip
import os
import psycopg2

# Constants
FILE_PATH = os.path.dirname(os.path.realpath(__file__))
CREDENTIALS = FILE_PATH + '/' + 'credentials'
REGION = 'us-west-2'
BUCKET = 'landsat-pds'
NEW_SCENE_LIST = 'scene_list.gz'
OLD_SCENE_LIST = FILE_PATH + '/' + 'scene_list'
NEW_SCENE_LIST_NAME = 'scene_list_{}'.format(date.today().strftime('%Y%m%d'))
NEW_SCENE_LIST_NAME_GZ = '{}.gz'.format(NEW_SCENE_LIST_NAME)
DIFF = FILE_PATH + '/' + 'diff.csv'
TEMP_TABLE = 'path_row_temp'
TABLE = 'test'
SEP = ','


# def create_diff():
#     with gzip.open(NEW_SCENE_LIST_NAME_GZ) as new_scene_list_file_object, \
#             open(OLD_SCENE_LIST, 'rb') as old_scene_list_file_object:

#         # Load new scene list file
#         new_scene_list = new_scene_list_file_object.readlines()

#         # Load old scene list file
#         old_scene_list = old_scene_list_file_object.readlines()

#         # Calculate diffrence between old and new scene lists
#         diff = list(set(new_scene_list) - set(old_scene_list))

#     # Write diff to csv file
#     with open(DIFF, 'wb') as dif:
#         dif.writelines(diff)

#     return dif, new_scene_list, diff

# def diff_to_db(dif, cur, conn):
#     with open(DIFF, 'rb') as dif:
#         # Copy diff to DB
#         cur.copy_from(dif, TABLE, sep=SEP)

#         # Commit changes
#         conn.commit()
def get_credentials():
    # Get credentials
    with open(CREDENTIALS, 'rb') as cred:
        creds = cred.readlines()

    # Assign credentials
    AWS_ACCESS_KEY_ID = creds[0].rstrip('\n')
    AWS_SECRET_ACCESS_KEY = creds[1].rstrip('\n')
    DATABASE_URL = creds[2].rstrip('\n')

    return AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DATABASE_URL


def get_new_scene_list(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
    # Make connection to S3
    S3conn = connect_to_region(REGION,
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Get landsat-pds bucket
    landsat_pds_bucket = S3conn.get_bucket(BUCKET)

    # Get new scene list file key
    scene_list_key = landsat_pds_bucket.get_key(NEW_SCENE_LIST)

    # Get new scene list file
    scene_list_key.get_contents_to_filename(NEW_SCENE_LIST_NAME_GZ)


def connect_to_db(DATABASE_URL):
    # Connect to DB
    conn = psycopg2.connect(DATABASE_URL)

    # Create DB cursor
    cur = conn.cursor()

    return cur, conn


def check_path_row_size(cur, conn):
    # Command
    command = "SELECT COUNT (*) FROM {table}".format(table=TABLE)
    cur.execute(command)
    size_tuple = cur.fetchone()
    return size_tuple[0]


def update_path_row_temp(cur, conn):
    with gzip.open(NEW_SCENE_LIST_NAME_GZ) as new_scene_list_file_object:
        # Copy new scene list to temp table in DB
        cur.copy_from(new_scene_list_file_object, TEMP_TABLE, sep=SEP)

        # Commit changes
        conn.commit()


def update_path_row(cur, conn):
    command = """INSERT INTO {table} SELECT * FROM {temp_table}
                 WHERE
                 not exists(SELECT * FROM {table}
                            WHERE acquisitiondate={temp_table}.acquisitiondate)
                """.format(table=TABLE, temp_table=TEMP_TABLE)
    cur.execute(command)

    # Commit changes
    conn.commit()


def delete_path_row_temp(cur, conn):
    command = """DELETE FROM {temp_table}""".format(temp_table=TEMP_TABLE)
    cur.execute(command)

    # Commit changes
    conn.commit()


def write_to_update_log(cur, conn, date_time, event, state, quantity=None):
    # Command
    command = """INSERT INTO path_row_update_log (datetime, event, state, quantity)
                 VALUES ('{date_time}', '{event}', '{state}', '{quantity}')
                 """.format(date_time=date_time,
                            event=event,
                            state=state,
                            quantity=quantity)
    cur.execute(command)

    # Commit changes
    conn.commit()


def close_db_connection(cur, conn):
    # Close communication with the database
    cur.close()
    conn.close()


def main():
    # Get credentials
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DATABASE_URL = get_credentials()

    # Connect to db
    cur, conn = connect_to_db(DATABASE_URL)

    # Check size of path_row table
    size_old = check_path_row_size(cur, conn)
    write_to_update_log(cur,
                        conn,
                        datetime.utcnow(),
                        'pre update size',
                        5,
                        size_old)

    # Get new scene list
    try:
        get_new_scene_list(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        write_to_update_log(cur,
                            conn,
                            datetime.utcnow(),
                            'get new scene list',
                            5)
    except:
        write_to_update_log(cur,
                            conn,
                            datetime.utcnow(),
                            'get new scene list',
                            10)

    # Delete path_row_temp
    try:
        delete_path_row_temp(cur, conn)
        write_to_update_log(cur,
                            conn,
                            datetime.utcnow(),
                            'clean path_row_temp',
                            5)
    except:
        write_to_update_log(cur,
                            conn,
                            datetime.utcnow(),
                            'clean path_row_temp',
                            10)

    # Push new scene list to temp
    try:
        update_path_row_temp(cur, conn)
        write_to_update_log(cur,
                            conn,
                            datetime.utcnow(),
                            'push new scene list to temp',
                            5,
                            cur.rowcount)
    except:
        write_to_update_log(cur,
                            conn,
                            datetime.utcnow(),
                            'push new scene list to temp',
                            10)

    # Update path_row from path_row_temp
    try:
        update_path_row(cur, conn)
        write_to_update_log(cur,
                            conn,
                            datetime.utcnow(),
                            'update path_row from path_row_temp',
                            5,
                            cur.rowcount)
    except:
        write_to_update_log(cur,
                            conn,
                            datetime.utcnow(),
                            'update path_row from path_row_temp',
                            10)

    # # Overwrite scene list file with new data
    # try:
    #     with open(OLD_SCENE_LIST, 'wb') as old:
    #         old.writelines(new_scene_list)
    #     write_to_update_log(cur,
    #                         conn,
    #                         datetime.utcnow(),
    #                         'overwrite old scene list with new',
    #                         5)
    # except:
    #     write_to_update_log(cur,
    #                         conn,
    #                         datetime.utcnow(),
    #                         'overwrite old scene list with new',
    #                         10)

    # # Remove new scene list file
    # try:
    #     os.remove(NEW_SCENE_LIST_NAME_GZ)
    #     write_to_update_log(cur,
    #                         conn,
    #                         datetime.utcnow(),
    #                         'remove old scene list',
    #                         5)
    # except:
    #     write_to_update_log(cur,
    #                         conn,
    #                         datetime.utcnow(),
    #                         'remove old scene list',
    #                         10)

    # Check size of path_row table
    size_new = check_path_row_size(cur, conn)
    write_to_update_log(cur,
                        conn,
                        datetime.utcnow(),
                        'post update size',
                        5,
                        size_new)

    # Check difference of size of path_row table
    size_diff = size_new - size_old
    write_to_update_log(cur,
                        conn,
                        datetime.utcnow(),
                        'actual diff size',
                        5,
                        size_diff)

    # Close conneciton to db
    close_db_connection(cur, conn)

if __name__ == '__main__':
    main()
