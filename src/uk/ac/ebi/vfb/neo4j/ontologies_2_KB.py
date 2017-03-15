'''
Created on Mar 15, 2017

@author: davidos
'''

from .KB_tools import node_importer
from ..curie_tools import map_iri
import sys

ni = node_importer(sys.argv(1), sys.argv(2), sys.argv(3))

bebop = 'http://ontologies.berkeleybop.org'
obo = map_iri('obo')
vfb = obo + 'fbbt/vfb/'
ontologies = [bebop + 'fbbt/fbbt-simple.json', 
              bebop + 'so.json', 
              vfb + 'vfb_ext.json']
for o in ontologies:
    ni.update_from_obograph(url = o)
    ni.commit()