'''
Created on Mar 15, 2017

@author: davidos
'''

from uk.ac.ebi.vfb.neo4j.KB_tools import node_importer
from uk.ac.ebi.vfb.curie_tools import map_iri
import sys

ni = node_importer(sys.argv[1], sys.argv[2], sys.argv[3])
ni.add_default_constraint_set(['Class', 'Individual'])

bebop = 'http://ontologies.berkeleybop.org/' # This should be a temporary expedient.
vfb_gh = 'https://raw.githubusercontent.com/VirtualFlyBrain/VFB_owl/master/src/owl/'  # Until next release!
obo = map_iri('obo')
vfb = obo + 'fbbt/vfb/'
ontologies = [bebop + 'fbbt/fbbt-simple.json', 
              bebop + 'so.json', 
              bebop +  'fbbi.json',
              vfb_gh + 'vfb_ext.json']
    
for o in ontologies:
    print("Loading %s" % o)
    ni.update_from_obograph(url = o)
    ni.commit()
