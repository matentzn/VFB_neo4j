'''
Created on Mar 6, 2017

@author: davidos
'''

iri_map = {'obo': 'http://purl.obolibrary.org/obo/',
           'vfb': 'http://www.virtualflybrain.org/owl/',
           'fb' : 'http://flybase.org/reports/'}

def map_iri(curie):
    return iri_map[curie]