import time
import string
import config
import json
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth



print "Hello world"

print config.consumer_key


keyword = ['New York', 'Los Angeles', 'San Francisco', 'Seattle', 'Dallas', 'Washington', 'Boston', 'Atlanta',
           'Chicago', 'Las Vegas']

# This is a basic listener that just prints received tweets to stdout.
class Listener(StreamListener):
    def __init__(self):
        self.whatever = 0


    def on_data(self, data):
        try:
            f = open('streamingData.json', 'w')
            f.write(data)
            # print data
            f.close()
            self.read_json_file()
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(5)

        return True

    def on_error(self, status):
        print(status)
        return True

    def read_json_file(self):
        tweets_filename = 'streamingData.json'
        tweets_file = open(tweets_filename, "r")
        for line in tweets_file:
            try:
                tweet = json.loads(line.strip())

                if 'text' in tweet and 'user' in tweet and 'coordinates' in tweet:  # only messages contains 'text' field is a tweet
                    kw = "undefined"
                    for word in keyword:
                        if tweet['text'].find(word) != -1:
                            kw = word
                            break
                    document = {"text":tweet['text'], "keyword":kw}
                    if tweet['geo'] is not None and tweet['geo'] != "null" and 'user' in tweet and 'name' in tweet['user']:
                        document['pin'] = {"location":{"lat":tweet['geo']['coordinates'][0],"lon":tweet['geo']['coordinates'][1]}}
                        document['name'] = {"name": tweet['user']['name']}
                        es.index(index="my_location", doc_type="location", id=tweet["id_str"], body=document)

                        print(es.get(index="my_location", doc_type="location", id=tweet["id_str"]))

                    # else:
                    #
                    #     print "None!!"

            except:
                continue
        tweets_file.close()

if __name__ == '__main__':
    AWS_ACCESS_KEY = 'AKIAJLTA4HFHSGQUFYSQ'
    AWS_SECRET_KEY = 'kcjVgO9bWnYS7sQYBldDJUhu0kLVFfiB31R3+m/e'
    region = 'us-east-2'  # For example, us-east-1
    service = 'es'

    awsauth = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, region, service)

    host = 'search-twitter-test-2-zx2ikzsfpdsbfukjjlmcsxuexy.us-east-2.es.amazonaws.com'  # For example, my-test-domain.us-east-1.es.amazonaws.com

    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )


    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_token_secret)


    twitter_stream = Stream(auth, Listener())

    while True:
        try:
            twitter_stream.filter(track=keyword)

        except:
            print "Fatal Error!"



