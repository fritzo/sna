#!/usr/bin/python

import twitter # Python Twitter Tools (not the homonymous Python-Twitter)
import urllib2  # for .quote, .unquote
import sqlite3
from matplotlib import pyplot
import main

def TODO (message = None): raise NotImplementedError(message)

search = twitter.Twitter(domain='search.twitter.com').search

class CachedSearcher:

  def __init__ (self):

    self.search = twitter.Twitter(domain='search.twitter.com').search

    self.db = sqlite3.connect('data/bands.sqlite3')
    self.db_cursor = self.db.cursor()

    self.update_db()

  def update_db (self):

    execute = self.db_cursor.execute

    tables = [ row[0] for row in execute(
      '''
      select name
      from sqlite_master
      where type='table'
      order by name
      ''',
      ))]

    print tables # DEBUG

    if 'searches' not in tables:
      execute(
        '''
        create table searches (
          query text,
          latest_id int
          )
        ''')

    if 'search_results' not in tables:
      execute(
        '''
        create table search_results (
          query text,
          tweet_id int
          )
        ''')

    if 'tweets' not in tables:
      execute(
        '''
        create table tweets (
          tweet_id int,
          from_id int,
          to_id int,
          geo text,
          created_at text,
          text text
          )
        ''')

  def get_latest_id (self):

    rows = execute(
      '''
      select latest_id from searches where query='?'
      ''',
      (query,))

    if rows:

      return row[0]

    else:

      execute(
        '''
        update searches
        set latest_id=0
        where query='?'
        ''',
        (query,)
        )

      return 0

  def add_new_results (self, query):

    since_id = self.get_latest_id(query)

    execute = self.db_cursor.execute

    tweets = []
    for page_num in range(1,150):

      page = self.search(
          q = urllib2.quote(query),
          tweet_type = 'recent',
          with_twitter_user_id = True,
          page = page_num,
          since_id = since_id,
          )

      print page # DEBUG
      new_tweets = page['results']

      if new_tweets:
        tweets += new_tweets
        since_id = max(since_id, page['max_id'])
      else:
        break

    if tweets:

      execute(
        '''
        update searches
        set since_id='?'
        where query='?'
        ''',
        (since_id, query)
        )

      for tweet in tweets:
        execute(
          '''
          insert into tweets
          set 
            tweet_id='?'
            from_id='?'
            to_id='?'
            geo='?'
            created_at='?'
            text='?'
          ''',
          ( tweet['id'],
            tweet['from_id'],
            tweet['to_id'],
            tweet['geo'],
            tweet['created_at'],
            tweet['text'],
            )
          )

  def __call__ (query):

    execute = self.db_cursor.execute

    self.add_new_results(query)

    # return new + old results

    results = list(c.execute(
      '''
      select *
      from search_results inner join tweets
      order by tweet_id
      ''',
      ))

    if main.at_top():
      for result in results:
        print 

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
  print 'id: %s' % t.id
  print 'from_user_id: %s' % t.from_user_id
  print 'to_user_id: %s' % t.to_user_id
  print 'created_at: %' % t.created_at
  print 'geo: %' % t.geo
  print 'text: %s' % t.text

@main.command
def local_search (query, location = 'seattle'):
  'searches a phrase in a specific location'

  print query
  urllib2.quote(query)

  results = []
  for page_num in range(1,150):
    page = search(
        q = query,
        near = location,
        result_type = 'recent',
        with_twitter_user_id = True,
        page = page_num,
        )
    print page # DEBUG
    if page:
      results += page
    else:
      break

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

