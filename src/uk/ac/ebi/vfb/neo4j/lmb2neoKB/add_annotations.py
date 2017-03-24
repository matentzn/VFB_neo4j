'''
Created on Mar 6, 2017

@author: davidos
'''
from uk.ac.ebi.vfb.neo4j.lmb2neoKB.lmb_query_tools import get_conn
from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer
from uk.ac.ebi.vfb.curie_tools import map_iri
import sys

c = get_conn(usr = sys.argv[4], pwd = sys.argv[4])
cursor = c.cursor()
edge_writer = kb_owl_edge_writer(endpoint=sys.argv[1], usr=sys.argv[2], pwd=sys.argv[3])
vfb = map_iri('vfb')
obo = map_iri('obo')

cursor.execute("SELECT ind.shortFormID as iID, " \
            "objont.baseURI AS relBase, " \
            "rel.shortFormID as rel, " \
            "objont.baseURI as clazBase, " \
            "oc.shortFormID as claz " \
            "FROM owl_individual ind " \
            "JOIN neuron n ON (ind.uuid = n.uuid) " \
            "JOIN annotation a ON (n.idid=a.neuron_idid) " \
            "JOIN annotation_key_value akv ON (a.annotation_class = akv.annotation_class) " \
            "JOIN annotation_type ote ON (akv.id=ote.annotation_key_value_id) " \
            "JOIN owl_type ot on (ote.owl_type_id=ot.id) " \
            "JOIN owl_class oc ON (ot.class=oc.id) " \
            "JOIN owl_objectProperty rel ON (ot.objectProperty=rel.id) " \
            "JOIN ontology objont ON (objont.id = oc.ontology_id) " \
            "JOIN ontology relont ON (relont.id = rel.ontology_id) " \
            "WHERE a.text=akv.annotation_text" )

for d in cursor.fetchall():
    if d['rel']:  
        edge_writer.add_anon_type_ax(s = vfb + d['iID'], 
                                     r = d['relBase'] + d['rel'], 
                                     o = d['clazBase'] + d['claz'])
    else:
        edge_writer.add_named_type_ax(s = vfb + d['iID'], 
                                      o = d['clazBase'] + d['claz'])
    
edge_writer.commit(verbose = True)
edge_writer.test_edge_addition()
cursor.close()
c.close()
