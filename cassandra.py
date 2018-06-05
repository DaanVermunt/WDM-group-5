from cassandra.cluster import Cluster
from collections import Counter
from itertools import combinations
#import matplotlib.pyplot as plt
import csv
import pandas as pd
import numpy as np
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re

cluster = Cluster()
session = cluster.connect()
session = cluster.connect('wdm')
session.set_keyspace('wdm')
#query 3-popular state
rows = session.execute('SELECT state,city,month FROM popularstate')
pop_state = []
temp = []
for user_row in rows:
    state = user_row.state
    month = user_row.month
    temp = [state, month]
    pop_state.append(temp)

popState = Counter()
for sub in pop_state:
    for comb in combinations(sub,2):
        popState[comb] += 1

print(popState.most_common())

#popular city
rows = session.execute('SELECT state,city,month FROM popularcity')
pop_state = []
pop_city = []
temp = []
for user_row in rows:
    state = user_row.state
    city = user_row.city
    month = user_row.month
    temp = [state, month]
    pop_state.append(temp)
    temp = [city, state, month]
    pop_city.append(temp)
popCity = Counter()
for sub in pop_city:
    for comb in combinations(sub,3):
        popCity[comb] += 1

print(popCity.most_common())

#query 5-analysis of type of room over time
rows = session.execute('SELECT year,room_type FROM roomtrend')
types = []
rooms = ['Entire home', 'Private room', 'Shared room']
for row in rows:
    #room = rooms[int(row.room_type)-1]
    room = row.room_type
    year = row.year
    temp = [room, year]
    types.append(temp)

popType = Counter()
for sub in types:
    for comb in combinations(sub,2):
        popType[comb] += 1

types_count = popType.most_common()
with open('data1.csv', 'w') as myfile:
    wr = csv.writer(myfile)
    wr.writerow(types_count)

#query 1-frequent words in each listing
rows = session.execute('SELECT listing_id,comments FROM reviews')
reviews = []
for row in rows:
    listing = row.listing_id
    comment = row.comments
    temp = [listing, comment]
    reviews.append(temp)

labels = ['id', 'review']
reviewall = np.array(reviews)
df = pd.DataFrame(reviewall, columns=labels)
df[['id']] = df[['id']].apply(pd.to_numeric)
df[['review']] = df[['review']].astype(str)
df1 = df.groupby('id', as_index=False).agg(lambda x: ','.join(x))
df1['w1'] = 'NA'
df1['w2'] = 'NA'
df1['w3'] = 'NA'
df1['w4'] = 'NA'
df1['w5'] = 'NA'
stop_words = set(stopwords.words('english'))
stop_words.add('I')
stop_words.add('The')
stop_words.add('\'s')
#stop_words.add('She')
for i in range(0,(len(df1)-1)):
    nstr = re.sub(r'[?|$|.|!|,]',r'',df1['review'][i])
    word_tokens = word_tokenize(nstr)
    filtered_sentence = [w for w in word_tokens if not w in stop_words]
    filtered_sentence = []
    for w in word_tokens:
        if w not in stop_words:
           filtered_sentence.append(w)
    fre = Counter(" ".join(filtered_sentence).split()).most_common(5)
    l = len(fre)
    if l>4:
       df1['w1'][i] = fre[0][0]
       df1['w2'][i] = fre[1][0]
       df1['w3'][i] = fre[2][0]
       df1['w4'][i] = fre[3][0]
       df1['w5'][i] = fre[4][0]
    elif l>3:
        df1['w1'][i] = fre[0][0]
        df1['w2'][i] = fre[1][0]
        df1['w3'][i] = fre[2][0]
        df1['w4'][i] = fre[3][0]
    elif l>2:
        df1['w1'][i] = fre[0][0]
        df1['w2'][i] = fre[1][0]
        df1['w3'][i] = fre[2][0]
    elif l>1:
        df1['w1'][i] = fre[0][0]
        df1['w2'][i] = fre[1][0]
    else:
        df1['w1'][i] = fre[0][0]


