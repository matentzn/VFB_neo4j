'''
Created on Mar 6, 2017

@author: davidos
'''

from .lmb_query_tools import get_conn
from ..KB_tools import kb_owl_edge_writer
from ...curie_tools import map_iri
import sys

c = get_conn(usr = sys.argv[4], pwd = sys.argv[4])
cursor = c.cursor()
edge_writer = kb_owl_edge_writer(endpoint=sys.argv[1], usr=sys.argv[2], pwd=sys.argv[3])
### Query for cluster inds (version 3)
vfb = map_iri('vfb')
obo = map_iri('obo')

cursor.execute("SELECT DISTINCT ind.shortFormID as cvid, c.cluster as cnum, " \
               "eind.shortFormID as evid, c.clusterv as cversion " \
               "FROM owl_individual ind " \
               "JOIN cluster c ON (ind.uuid=c.uuid) " \
               "JOIN clustering cg ON (cg.cluster=c.cluster) " \
               "JOIN neuron n ON (cg.exemplar_idid=n.idid) " \
               "JOIN owl_individual eind ON (n.uuid=eind.uuid) " \
               "WHERE cg.clusterv_id = c.clusterv " \
               "AND ind.type_for_def  = 'cluster' " \
               "AND c.clusterv = '3'")
    
# But are Individuals already present?
# Yes, but they don't have names!  These need to be rolled as version.cluster.
# Consider: Adding clustering version as attribute.
    
for d in cursor.fetchall():
#       vfb_ind.addNamedIndividual(d["cvid"])
    edge_writer.statements.append("MATCH (c:Individual { IRI: '%s' }) " \
                                  "SET c.label = '%s'" 
                                  % (d['cvid'], 
                                     "cluster " + str(d['cversion']) + '.' + str(d['cnum'])))
    edge_writer.add_named_type_ax(s =vfb + 'VFB_10000005', 
                         o = vfb + d["cvid"])
#       vfb_ind.label(d["cvid"], "cluster " + str(d["cversion"]) + "." + str(d["cnum"])) # Note ints returned by query need to be coerced into strings.
    edge_writer.add_fact(s = vfb + d["evid"], 
                r = vfb + "c099d9d6-4ef3-11e3-9da7-b1ad5291e0b0",
                o = vfb + d["cvid"]) # UUID for exemplar as a placeholder - awaiting addition to RO
    edge_writer.add_fact(s = d["cvid"], 
                r = vfb + "C888C3DB-AEFA-447F-BD4C-858DFE33DBE7", 
                o = vfb + d["evid"]) # UUID for exemplar as a placeholder - awaiting addition to RO
edge_writer.commit(verbose= True)


def map_to_clusters(cursor, vfb_ind):
    """Maps fc individuals to clusters"""

#    oe_check_db_and_add("RO_0002351", 'owl_objectProperty', cursor, vfb_ind) #  has_member
#    oe_check_db_and_add("RO_0002350", 'owl_objectProperty', cursor, vfb_ind)  #  member_of

    cursor.execute("SELECT DISTINCT cind.shortFormID AS cvid, nind.shortFormID AS mvid " \
                   "FROM clustering cg " \
                   "JOIN neuron n ON (cg.idid=n.idid) " \
                   "JOIN owl_individual nind ON (n.uuid=nind.uuid) " \
                   "JOIN cluster c ON (cg.cluster=c.cluster) " \
                   "JOIN owl_individual cind ON (c.uuid=cind.uuid) " \
                   "WHERE c.clusterv = cg.clusterv_id " \
                   "AND cg.clusterv_id = '3'") # It is essential to set clustering version twice ! (crappy schema...)

    # Now add cluster assertions.  Note - these are declared in both directions as elk cannot cope with inverses.

    for d in cursor.fetchall():
        edge_writer.add_fact(s = vfb + d['cvid'],
                  obo +"RO_0002351" , 
                  vfb + d['mvid'])
        edge_writer.add_fact(s = vfb + d['mvid'],
                  obo +"RO_0002350" , 
                  vfb + d['cvid'])        
    cursor.close()

edge_writer.commit(verbose=True)
c.close()
    
    

    
    
    
    