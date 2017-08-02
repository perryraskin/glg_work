"""
Talk to GLGLIVE. This is super environment variable driven. Inline since these
are really big queries, so epi seems like a poor choice.
"""
import collections
import json
import os
import pandas as pd
import pymssql
import sys
IdText = collections.namedtuple('IdText', ['id', 'text'])
def pandas_the_data(queryfile, index_col=None):
    """
    Turn a file of sql into a wonderful pandas dataframe.
    """
    with open(queryfile, 'r') as queryfile:
        query = queryfile.read()
    with pymssql.connect(
        server=os.environ['DATABASE_DATAHUB_SERVER'],
        user=os.environ['DATABASE_DATAHUB_USER'],
        password=os.environ['DATABASE_DATAHUB_PASSWORD'],
        port=os.environ['DATABASE_DATAHUB_PORT']
    ) as conn:
        return pd.read_sql(query, conn, index_col=index_col)
def stream_the_data(queryfile, argumentfile=None):
    """
    Generator to get a nice pile of rows turned into tuples.
    """
    with open(queryfile, 'r') as queryfile:
        query = queryfile.read()
    if argumentfile:
        with open(argumentfile, 'r') as argumentfile:
            arguments = json.loads(argumentfile.read())
        display(arguments, file=sys.stderr)
        query = query.format(**arguments)
    display(query, file=sys.stderr)
    with pymssql.connect(
        server=os.environ['DATABASE_DATAHUB_SERVER'],
        user=os.environ['DATABASE_DATAHUB_USER'],
        password=os.environ['DATABASE_DATAHUB_PASSWORD'],
        port=os.environ['DATABASE_DATAHUB_PORT']
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        yield [column[0] for column in cursor.description]
        for row in cursor:
            yield row
def id_text_sample(rowstream):
    """
    Combine a every row into an IdText tuple from a given generator.
    """
    for row in rowstream:
        yield {'id': row[0], 'text': ' '.join(row[1:])}
def say_hi():
  print('hello')