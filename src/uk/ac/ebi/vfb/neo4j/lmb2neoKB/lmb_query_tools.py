'''
Created on Mar 6, 2017

@author: davidos
'''
import pymysql

## Requires ssh tunnel

def get_conn(usr, pwd):
    return pymysql.connect(host='localhost', user=usr, db=pwd, 
                    cursorclass=pymysql.cursors.DictCursor, port = 3307, 
                    charset='utf8mb4',
                    password = 'flycircuit')
    
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
