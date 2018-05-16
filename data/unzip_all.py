import gzip
import shutil

cities = ['Asheville', 'Austin', 'Boston', 'Chicago', 'Denver', 'Los_Angeles', 'Nashville', 'New_Orleans',
          'New_York_City', 'Oakland', 'Portland', 'San_Diego', 'San_Fransisco', 'Santa_Cruz', 'Seattle',
          'Washington']
file_types = ['calendar', 'listings', 'reviews']

for city in cities:
    for file_type in file_types:
        file_in = city + '/' + file_type + '.csv.gz'
        file_out = city + '/' + file_type + '.csv'
        with gzip.open(file_in, 'rb') as f_in:
            with open(file_out, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    print(city)

