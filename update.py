download new scene list
gunzip  new scene list
sort -n new scene list
sort -n sort old list
comm -2 -3 newscenelist_sorted oldscenelist_sorted > diff.csv
cp diff to RDS
rm oldscenelist
mv newscenelist > oldscenelist
