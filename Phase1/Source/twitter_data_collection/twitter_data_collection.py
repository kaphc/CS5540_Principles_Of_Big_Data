import tweepy
import csv

consumerKey = '69SNKxw0qbGY6PPZBcRVLZVpP'
consumerSecret = 'sIUppqVb3mDXDdbnXGst50fL2DvmtcyjakxbJhA7D5vQpt3PNr'
accessToken = '1039586617445572608-3R3OHxxrH5JY8e9IITG3w3pwkZpGFc'
accessTokenSecret = 'KrqdNUh5tddGq1SW1SUIeQMBelQcyLzQVOaOzAPfNZG84'

if __name__ == '__main__':

    # twitter authentication
    auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessTokenSecret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # create a new csv file
    csvCursor = open('/home/kavin/PycharmProjects/CS5540_Principle_of_Big_Data/Phase1/data/master_tweets.csv', 'a')

    # use csv writer to write into csv data
    csvWriter = csv.writer(csvCursor)

    # extract tweets containing parse words
    parse_words = ['disease', 'outbreak']
    for parse_word in parse_words:

        # to collect tweets from 2000 and collect 100K
        for tweet in tweepy.Cursor(api.search, q=parse_word, count=100000, lang="en", since="2000-01-01", include_entities=True).items():
            csvWriter.writerow([tweet.created_at, tweet.entities, tweet.text.encode('utf-8')])

    # close csv file
    csvCursor.close()