#!/usr/bin/python

import time
import twitter # Python Twitter Tools (not the homonymous Python-Twitter)
import sqlite3
from matplotlib import pyplot
import main

def TODO (message = None): raise NotImplementedError(message)

def zero_if_none (x):
  if x is None: return 0
  else:         return x
def null_if_none (x):
  if x is None: return ''
  else:         return x

search = twitter.Twitter(domain='search.twitter.com').search

class CachedSearcher:

  def __init__ (self, database = '/data/sna/bands.db'):

    self.twitter_search = twitter.Twitter(domain='search.twitter.com').search

    self.db = sqlite3.connect(database)
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

      page = self.twitter_search(
          q = query,
          tweet_type = 'recent',
          with_twitter_user_id = True,
          page = page_num,
          since_id = old_since_id,
          )

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
            '', # TODO remove geo column
            tweet['created_at'],
            tweet['text'])
         for tweet in tweets ]

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

    return len(tweets)

  def search (self, query):

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

  def update_db (self):

    queries = list(self.db_cursor.execute('select query from searches'))

    print time.asctime()

    for row in queries:
      query = row[0]
      num_tweets = self.add_new_results(query)
      print 'added %i new tweets about %r' % (num_tweets, query)

  def get_queries (self):

    queries = list(self.db_cursor.execute(
      'select count(*),query from search_results group by query'))
    queries.sort()
    queries.reverse()

    return queries

#----( commands )-------------------------------------------------------------

searcher = CachedSearcher()

@main.command
def search (query):
  'searches twitter & caches result in local database'

  print query
  results = searcher.search(query)

  if main.at_top():
    for t in results:
      print '-' * 80
      print t
    print '\nsearch returned %i results' % len(results)
  else:
    return results

@main.command
def update ():
  'updates all search terms'

  searcher.update_db()

@main.command
def queries ():
  'lists cached queries'
  print 'cached queries:'
  for row in searcher.get_queries():
    print '  %d\t%s' % row

if __name__ == '__main__': main.main()

