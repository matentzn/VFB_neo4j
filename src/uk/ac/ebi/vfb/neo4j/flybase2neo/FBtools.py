#!/usr/bin/env python3
import psycopg2

'''
Created on 4 Feb 2016

@author: davidos
'''


def dict_cursor(cursor):
    """Takes cursor as an input, following execution of a query, returns results as a list of dicts"""
    # iterate over rows in cursor.description, pulling first element
    description = [x[0] for x in cursor.description] 
    l = []
    for row in cursor: # iterate over rows in cursor
        d = dict(zip(description, row))
#    yield dict(zip(description, row))  # This yields an iterator.  Doesn't actually run until needed.
        l.append(d)
    return l


def get_fb_conn():
    return psycopg2.connect(dbname = 'flybase', host ='chado.flybase.org', user = 'flybase')

def test():
    conn = get_fb_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM db")
    dc = dict_cursor(cursor)
    for d in dc:
        print(d.items())

    

