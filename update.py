import boto

# Define AWS credentials
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
REGION = 'us-west-2'
DATABASE_URL = os.environ.get('DATABASE_URL')

download new scene list
gunzip  new scene list
sort -n new scene list
sort -n sort old list
comm -2 -3 newscenelist_sorted oldscenelist_sorted > diff.csv
cp diff to RDS
rm oldscenelist
mv newscenelist > oldscenelist
