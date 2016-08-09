
from pymongo import MongoClient
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import datetime

# The MongoDB connection info. Database name is TwitterStream, and collection name is tweets.
#Have used mlab.com to store the data 
connection = MongoClient("mongodb://usname:pwd@ds019054.mlab.com:19054/twitterstream")
db = connection.twitterstream
db.tweets.create_index("id",unique=True)
collection = db.tweets

# Add the keywords to track. They can be cashtags, hashtags, or words.
keywords = ['$goog', '#election2016', 'US Election']

# Optional - Only grab tweets of specific language
language = ['en']

# replace these with your own values to get after creating an app on Twitter's developer portal.
consumer_key = ""
consumer_secret = ""
access_token=""
access_token_secret=""

# The below code will get Tweets from the stream and store only the important fields to the database
class StdOutListener(StreamListener):

    def on_data(self, data):

        # Load the Tweet into the variable "t"
        t = json.loads(data)
        # Pull important data from the tweet to store in the database.
        tweet_id = t['id_str']  # The Tweet ID from Twitter in string format
        username = t['user']['screen_name']  # The username of the Tweet author
        followers = t['user']['followers_count']  # The number of followers the Tweet author has
        text = t['text']  # The entire body of the Tweet
        hashtags = t['entities']['hashtags'] # Any hashtags used in the Tweet
        urls = t['entities']['urls'] # Any URLs used in tweet
        dt = t['created_at']  # The timestamp of when the Tweet was created
        language = t['lang']  # The language of the Tweet

        # Convert the timestamp string given by Twitter to a date object called "created". This is more easily manipulated in MongoDB.
        created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')

        # Load all of the extracted Tweet data into the variable "tweet" that will be stored into the database
        tweet = {'id':tweet_id, 'username':username, 'followers':followers, 'text':text, 'hashtags':hashtags, 'language':language, 'created':created}

        # Save the refined Tweet data to MongoDB
        collection.save(tweet)

        # Optional - Print the username and text of each Tweet to the console in realtime as they are pulled from the stream
        print username + ':' + ' ' + text
        return True

    # Prints the reason for an error to the console
    def on_error(self, status):
        print status

# Some Tweepy code that can be left alone. It pulls from variables at the top of the script
if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=keywords, languages=language)
