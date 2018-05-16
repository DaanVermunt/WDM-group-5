import requests

nyc_review_url = 'http://data.insideairbnb.com/united-states/ny/new-york-city/2018-03-04/data/reviews.csv.gz'
filename = 'New_York_City/reviews.csv.gz'

with open(filename, "wb") as f:
    r = requests.get(nyc_review_url)
    f.write(r.content)