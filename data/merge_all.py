import csv
import time
import sys
import itertools, sys

cities = ['Asheville','Austin', 'Boston', 'Chicago', 'Denver', 'Los_Angeles', 'Nashville', 'New_Orleans',
          'New_York_City', 'Oakland', 'Portland', 'San_Diego', 'San_Fransisco', 'Santa_Cruz', 'Seattle',
          'Washington']
file_types = ['calendar', 'listings', 'reviews']



spinner = itertools.cycle(['-', '\\', '|', '/'])

def spinner_spin():
    global spinner
    sys.stdout.write(spinner.next())  # write the next character
    sys.stdout.flush()                # flush stdout buffer (actual character display)
    sys.stdout.write('\b')            # erase the last written char
    return




# First determine the field names from the top line of each input file
fieldnames = []
part = 5

for file_type in file_types:
    file_out = file_type + '_' + str(part) + '.csv'
    
    for city in cities:
        file_in = city + '/' + file_type + '.csv'
        
        #for filename in inputs:
        with open(file_in, "r") as f_in:
            reader = csv.reader(f_in)
            headers = next(reader)
            for h in headers:
                if h not in fieldnames:
                    fieldnames.append(h)

    # Then copy the data
    with open(file_out, "w") as f_out:   # Comment 2 below
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        
        headerString = ""
        for name in fieldnames:
            headerString += name
            headerString += ','
            
        headerString = headerString[:-1]
        headerString += '\n'
        
        f_out.write(headerString)
    
        for city in cities:
            file_in = city + '/' + file_type + '.csv'   
            
            #for filename in inputs:
            with open(file_in, "r") as f_in:
                reader = csv.DictReader(f_in)  # Uses the field names in this file

                for line in reader:
                    writer.writerow(line)
                    spinner_spin()
            
            print(city)
                    
    print(file_type + "...\t\t Done")
