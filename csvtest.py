import csv
with open('map_onet_soc.csv' ) as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:
  # print(row['onet'], row['soc5'])
    

    print(row)
print (reader.get('99-9999.99'))
