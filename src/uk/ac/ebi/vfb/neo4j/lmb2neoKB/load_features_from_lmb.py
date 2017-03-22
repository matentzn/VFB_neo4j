'''
Created on Mar 15, 2017

@author: davidos
'''

from uk.ac.ebi.vfb.neo4j.lmb2neoKB.lmb_query_tools import get_conn
from uk.ac.ebi.vfb.neo4j.KB_tools import node_importer
import sys

c = get_conn(usr = sys.argv[4], pwd = sys.argv[4])
cursor = c.cursor()
ni = node_importer(endpoint=sys.argv[1], usr=sys.argv[2], pwd=sys.argv[3])
ni.add_default_constraint_set(['Feature'])

cursor.execute("SELECT oc.shortFormID FROM owl_class oc " \
               "JOIN ontology o on oc.ontology_id=o.id " \
               "WHERE o.short_name = 'fb_feat'")

features = [d['shortFormID'] for d in cursor.fetchall()]
    
ni.update_from_flybase(load_list = features)
ni.commit(verbose="True")
        
cursor.close()
c.close()
