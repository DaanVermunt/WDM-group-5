import random

# Asheville is left out because of a hack for the header
cities = ['Austin', 'Boston', 'Chicago', 'Denver', 'Los_Angeles', 'Nashville', 'New_Orleans',
          'New_York_City', 'Oakland', 'Portland', 'San_Diego', 'San_Fransisco', 'Santa_Cruz', 'Seattle',
          'Washington']
file_types = ['calendar', 'listings', 'reviews']

random.seed(a=42)
part = 5

for file_type in file_types:
    file_out = file_type + '_' + str(part) + '.csv'
    f_out = open(file_out, "a")

    # get Asheville first so the header is added once:
    file_in = "Asheville" + '/' + file_type + '.csv'
    for line in open(file_in):
        if random.randint(0, 99) < part:
            f_out.write(line)
    for city in cities:
        file_in = city + '/' + file_type + '.csv'

        # now the rest:
        f_in = open(file_in)
        f_in.next()  # skip the header
        for line in f_in:
            f_out.write(line)
        f_in.close()
    f_out.close()

    print(file_type)

