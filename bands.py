#!/usr/bin/python

import twitter # Python Twitter Tools (not the homonymous Python-Twitter)
import urllib2  # for .quote, .unquote
import sqlite3
from matplotlib import pyplot
import main

def TODO (message = None): raise NotImplementedError(message)

def zero_if_none (x):
  if x is None: return 0
  else:         return x

search = twitter.Twitter(domain='search.twitter.com').search

class CachedSearcher:

  def __init__ (self):

    self.search = twitter.Twitter(domain='search.twitter.com').search

    self.db = sqlite3.connect('data/bands.db')
    self.db_cursor = self.db.cursor()

    self.init_db()

  def init_db (self):

    tables = [ row[0] for row in self.db_cursor.execute(
      '''
      select name
      from sqlite_master
      where type = 'table'
      order by name
      ''',
      )]

    print tables # DEBUG

    if 'searches' not in tables:
      self.db_cursor.execute(
        '''
        create table searches (query text, latest_id long)
        ''')

    if 'search_results' not in tables:
      self.db_cursor.execute(
        '''
        create table search_results (query text, tweet_id long)
        ''')

    if 'tweets' not in tables:
      self.db_cursor.execute(
        '''
        create table tweets (
          tweet_id long,
          from_user_id long,
          to_user_id long,
          geo text,
          created_at text,
          text text)
        ''')

  def get_latest_id (self, query):

    rows = list(self.db_cursor.execute(
      '''
      select latest_id from searches where query=(?)
      ''',
      (query,)))

    if rows:

      return rows[0][0]

    else:

      self.db_cursor.execute(
        '''
        insert into searches (query, latest_id) values (?, 0)
        ''',
        (query,))

      return 0

  def add_new_results (self, query):

    since_id = self.get_latest_id(query)
    old_since_id = since_id

    tweets = []
    for page_num in range(1,150):

      page = self.search(
          q = urllib2.quote(query),
          tweet_type = 'recent',
          with_twitter_user_id = True,
          page = page_num,
          since_id = old_since_id,
          )

      print page # DEBUG
      new_tweets = page['results']

      if new_tweets:
        tweets += new_tweets
        since_id = max(since_id, page['max_id'])
      else:
        break

    if tweets:

      self.db_cursor.execute(
        '''
        update searches set latest_id = (?) where query = (?)
        ''',
        (since_id, query))

      tweets_data = [
          ( tweet['id'],
            zero_if_none(tweet['from_user_id']),
            zero_if_none(tweet['to_user_id']),
            zero_if_none(tweet['geo']),
            tweet['created_at'],
            tweet['text'])
         for tweet in tweets ]

      #DEBUG
      for tweet in tweets_data:
        print tweet

      self.db_cursor.executemany(
        '''
        insert into tweets
        (tweet_id, from_user_id, to_user_id, geo, created_at, text)
        values (?,?,?,?,?,?)
        ''',
        tweets_data)

      self.db_cursor.executemany(
        '''
        insert into search_results (query, tweet_id) values (?,?)
        ''',
        [ (query, tweet['id']) for tweet in tweets ])

    self.db.commit()

  def __call__ (self, query):

    self.add_new_results(query)

    # return new + old results

    results = list(self.db_cursor.execute(
      '''
      select
        tweets.tweet_id,
        tweets.from_user_id,
        tweets.to_user_id,
        tweets.geo,
        tweets.created_at,
        tweets.text
      from search_results join tweets
      on search_results.tweet_id = tweets.tweet_id
      where query = (?)
      order by tweets.tweet_id
      ''',
      (query,)))

    if main.at_top():
      for result in results:
        text = results[-1]
        print '\n%s' % text
    else:
      return results

search = CachedSearcher()

@main.command
def get_earwig_clients ():
  'returns list of synonyms of earwig clients'

  clients = [
      [ name.strip() for name in line.split('=') ]
      for line in open('data/earwig_clients.text').readlines()
      if not line.isspace()
      ]

  if main.at_top():
    for names in clients:
      print '\n  = '.join(names)
  else:
    return clients

def print_tweet (t):
  print t
  # print '''\
  # id: %s
  # from_user_id: %s
  # to_user_id: %s
  # created_at: %s
  # geo: %s
  # text: %s
  # ''' % t

@main.command
def cached_search (query):
  'searches twitter & caches result'

  print query
  results = search(query)

  if main.at_top():
    for t in results:
      print '-' * 80
      print_tweet(t)
  else:
    return results


@main.command
def init_db ():
  'initializes database of cached tweets'
  c = db.cursor()
  c.execute('''
      create table tweets
      ()
      ''')

@main.command
def help ():
  'just a test'
  pass

if __name__ == '__main__': main.main()

