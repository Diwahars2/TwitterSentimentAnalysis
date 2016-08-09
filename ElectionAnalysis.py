from pymongo import MongoClient
import pandas as pd
import json
from textblob import TextBlob
from bson.json_util import dumps
from bson import json_util, ObjectId
from pandas.io.json import json_normalize

connection=MongoClient("mongodb://vision:<v1s1on>@ds019054.mlab.com:19054/twitterstream")

db=connection.twitterstream

names=db.tweets



mongo_data = names.find()

sanitized = json.loads(json_util.dumps(mongo_data))

for i in sanitized:

# pass tweet into TextBlob
    tweet = TextBlob(i["text"])
    print tweet

# output sentiment polarity
    print tweet.sentiment.polarity

# determine if sentiment is positive, negative, or neutral
    if tweet.sentiment.polarity < 0:
      sentiment = "negative"
    elif tweet.sentiment.polarity == 0:
        sentiment = "neutral"
    else:
      sentiment = "positive"

# output sentiment
   # print sentiment
    connection1 = MongoClient("mongodb://vision:<v1s1on>@ds145395.mlab.com:45395/twittersentiment")
    db1 = connection1.twittersentiment
    #db.tweets.ensure_index("id", unique=True, dropDups=True)
    db1.sentiment.create_index("id",unique=True)
    collection = db.sentiment

    body={'author':i['username'],'message': i['text'],'polarity': tweet.sentiment.polarity,'subjectivity': tweet.sentiment.subjectivity,
                       'sentiment': sentiment}
       # return True
    collection.save(body)
    print i['username'] + ':' + ' ' + tweet.sentiment.polarity +''+ tweet.sentiment.subjectivity +''+ sentiment
# on failure
def on_error(self, status):
    print status