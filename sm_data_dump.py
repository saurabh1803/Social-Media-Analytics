#!/usr/bin/env python
# coding: utf-8


access_token = '1197604244-G9qyiTNfs9V1VPKtRkz8rv0YaxdMc9vPovNU6Dw'
access_token_secret = '9ahcXDBj76JQetdTxQL3hvyE6BoqRn9wfW1tUn9rCMmWf'
consumer_key = 'lKPbSIynHJeYcMfsiXIJuayCd'
consumer_secret = 'QDFkW07TwnNfm5sVP0poR1sqfe0FVrFQQD1qBQtx6K3kfSP67a'



import psycopg2
import tweepy 
import json
import datetime

def autorize_twitter_api():
    """
    This function gets the consumer key, consumer secret key, access token 
    and access token secret given by the app created in your Twitter account
    and authenticate them with Tweepy.
    """
    #Get access and costumer key and tokens
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    return auth

autorize_twitter_api()



def autorize_twitter_api():
    """
    This function gets the consumer key, consumer secret key, access token 
    and access token secret given by the app created in your Twitter account
    and authenticate them with Tweepy.
    """
    #Get access and costumer key and tokens
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    return auth

def create_tweets_table(term_to_search):
    """
    This function open a connection with an already created database and creates a new table to
    store tweets related to a subject specified by the user
    """
    
    #Connect to Twitter Database created in Postgres
    conn_twitter = psycopg2.connect(dbname="platform", user="ps", password="ps@123", host="172.20.1.51", port="6011")

    #Create a cursor to perform database operations
    cursor_twitter = conn_twitter.cursor()

    #with the cursor now, create two tables, users twitter and the corresponding table according to the selected topic
    cursor_twitter.execute("CREATE TABLE IF NOT EXISTS sm_user (id VARCHAR PRIMARY KEY, sm_user_name VARCHAR, sm_screen_name VARCHAR, sm_location VARCHAR, sm_follower_count int, sm_following_count int, sm_friends_count int, sm_favourite_count int, sm_post_count int, sm_profile_created_on timestamp, sm_time_zone VARCHAR, sm_geo_enabled VARCHAR, sm_profile_language VARCHAR, sm_text_url VARCHAR, sm_profile_image_url VARCHAR, sm_social_user_id VARCHAR, f_created_on timestamp, f_updated_on  timestamp, f_created_by VARCHAR , f_updated_by VARCHAR);")
    #cursor_twitter.execute("CREATE TABLE IF NOT EXISTS sm_base_text (id VARCHAR PRIMARY KEY,  sm_user_id int, sm_social_text_id int, sm_text VARCHAR, sm_source VARCHAR, sm_language VARCHAR, sm_quote_count int, sm_reply_count int, sm_shared_count int, sm_favorite_count int, f_created_on timestamp, f_updated_on  timestamp, f_created_by int, f_updated_by int);")
    #cursor_twitter.execute("CREATE TABLE IF NOT EXISTS sm_hashtag (sm_social_text_id VARCHAR PRIMARY KEY,sm_hashtag VARCHAR, f_created_on timestamp, f_updated_on  timestamp, f_created_by int, f_updated_by int);")
   
    #query_create = "CREATE TABLE IF NOT EXISTS %s (id SERIAL, created_at timestamp, tweet text NOT NULL, user_id VARCHAR, user_name VARCHAR, retweetstatus_user int, retweetstatus_name VARCHAR, PRIMARY KEY(id), FOREIGN KEY(user_id) REFERENCES twitter_users(user_id));" %("tweets_predict_"+term_to_search)
    #cursor_twitter.execute(query_create)
    
    #Commit changes
    conn_twitter.commit()
    
    #Close cursor and the connection
    cursor_twitter.close()
    conn_twitter.close()
    return


def store_tweets_in_table(dump_data):
    """
    This function open a connection with an already created database and inserts into corresponding table 
    tweets related to the selected topic
    """
    print(dump_data)
    #Connect to Twitter Database created in Postgres
    conn_twitter = psycopg2.connect(dbname="platform", user="ps", password="ps@123", host="172.20.1.51", port="6011")

    #Create a cursor to perform database operations
    cursor_twitter = conn_twitter.cursor()

    #with the cursor now, insert tweet into table
    cursor_twitter.execute("INSERT INTO sm_user (sm_user_name, sm_screen_name, sm_location, sm_follower_count, sm_following_count, sm_friends_count, sm_favourite_count, sm_post_count, sm_profile_created_on, sm_time_zone, sm_geo_enabled, sm_profile_language, sm_text_url, sm_profile_image_url, sm_social_user_id, f_created_on, f_updated_on, f_created_by, f_updated_by) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s);", (dump_data['sm_user_name'], dump_data['sm_screen_name'], dump_data['sm_location'], int(dump_data['sm_follower_count']), int(dump_data['sm_following_count']), int(dump_data['sm_friends_count']), int(dump_data['sm_favourite_count']), int(dump_data['sm_post_count']), dump_data['sm_profile_created_on'], dump_data['sm_time_zone'], dump_data['sm_geo_enabled'], dump_data['sm_profile_language'], dump_data['sm_text_url'], dump_data['sm_profile_image_url'], dump_data['sm_social_user_id'], dump_data['f_created_on'], dump_data['f_updated_on'], int(dump_data['f_created_by']), int(dump_data['f_updated_by'])))
    
    cursor_twitter.execute("INSERT INTO sm_base_text (sm_social_user_id, sm_social_text_id, sm_text, sm_source, sm_language, sm_quote_count, sm_reply_count, sm_shared_count, sm_favorite_count, f_created_on, f_updated_on, f_created_by, f_updated_by) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s);", (dump_data['sm_social_user_id'], dump_data['sm_social_text_id'], dump_data['sm_text'], dump_data['sm_source'], dump_data['sm_language'], dump_data['sm_quote_count'], dump_data['sm_reply_count'], dump_data['sm_shared_count'], dump_data['sm_favorite_count'], dump_data['f_created_on'], dump_data['f_updated_on'], dump_data['f_created_by'], dump_data['f_updated_by']))
    cursor_twitter.execute("INSERT INTO sm_hashtag (sm_text,sm_hashtag, f_created_on, f_updated_on, f_created_by, f_updated_by) VALUES (%s, %s,%s, %s,%s, %s);", (sm_social_text_id,sm_hashtag, f_created_on, f_updated_on, f_created_by, f_updated_by))

    #Commit changes
    conn_twitter.commit()
    count=cursor_twitter.rowcount
    #Close cursor and the connection
    cursor_twitter.close()
    conn_twitter.close()
    return count




class MyStreamListener(tweepy.StreamListener):
    
    '''
    def on_status(self, status):
        print(status.text)
    '''
        
    def on_data(self, raw_data):

#         try:
            global term_to_search
            
            data = json.loads(raw_data)            
#             print(data)
            dump_data = {}
            #Obtain all the variables to store in each column
            dump_data['sm_user_name'] = str(data['user']['name'])
            dump_data['sm_screen_name'] = str(data['user']['screen_name'])
            dump_data['sm_location'] = data['user']['location']
            dump_data['sm_follower_count'] = data['user']['followers_count']
            dump_data['sm_following_count'] = data['user']['followers_count']
            dump_data['sm_friends_count'] = data['user']['friends_count']
            dump_data['sm_favourite_count'] = data['user']['favourites_count']
            dump_data['sm_post_count'] = data['user']['statuses_count']
            dump_data['sm_profile_created_on'] = str(data['user']['created_at'])
            dump_data['sm_time_zone'] = str(data['user']['time_zone'])
            dump_data['sm_geo_enabled']=str(data['user']['geo_enabled'])
            dump_data['sm_profile_language']=str(data['user']['lang'])
            if len(data['entities']['urls']) > 0:
                print(data['entities'])
                dump_data['sm_text_url']=data['entities']['urls'][0]['expanded_url']
            else:
                 dump_data['sm_text_url'] = ""   
            dump_data['sm_profile_image_url']=data['user']['profile_image_url']
            dump_data['sm_social_user_id']=data['user']['id_str']
            dump_data['f_created_on']=str(datetime.datetime.now())
            dump_data['f_updated_on']=str(datetime.datetime.now())
            dump_data['f_created_by']=1
            dump_data['f_updated_by']=1
            dump_data['sm_social_text_id']=data['id_str']
            dump_data[' sm_text']=data['extended_tweet']['full_text']
            dump_data[' sm_source']=data['source']
            dump_data[' sm_language']=data['entities']['lang']
            dump_data[' sm_quote_count']=data['extended_tweet']['quote_count']
            dump_data[' sm_reply_count']=data['extended_tweet']['reply_count']
            dump_data[' sm_shared_count']=data['extended_tweet']['retweet_count']
            dump_data[' sm_favorite_count']=data['extended_tweet']['favorite_count']
            dump_data['sm_hashtag']=data['entities']['hashtags'][0]['text']
            
            hashtags = []   #make an empty list

            for hashtag in tweet["entities"]["hashtags"]:    #iterate over the list
            hashtags.append(hashtag["text"])             #append each hashtag to 'hashtags'

            c.execute("INSERT INTO news (timestamp, screen_name, created_at, id, text, hashtag_1) VALUES (%s,%s,%s,%s,%s,%s)", (time.time(), screen_name, created_at, identity, text, str(hashtags)))
            
            
            
            #Store them in the corresponding table in the database
            count=store_tweets_in_table(dump_data)        
            print(count)
#         except Exception as e:
           # print(e)
    
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False


if __name__ == "__main__": 
    #Creates the table for storing the tweets
    term_to_search = "VikashDubey"
#     create_tweets_table(term_to_search)
    print(datetime.datetime.now())
    #Connect to the streaming twitter API
    api = tweepy.API(wait_on_rate_limit_notify=True)
    
    #Stream the tweets
    streamer = tweepy.Stream(auth=autorize_twitter_api(), listener=MyStreamListener(api=api))
    streamer.filter(languages=["en"], track=[term_to_search])



