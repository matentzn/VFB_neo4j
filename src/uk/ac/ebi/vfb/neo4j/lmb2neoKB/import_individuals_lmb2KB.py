'''
Created on 13 Aug 2016

@author: davidos
'''
# A temporary expedient until import from OWL fixed, or we move to a native neo4J implementation of KB

from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer, node_importer
from uk.ac.ebi.vfb.curie_tools import map_iri
from uk.ac.ebi.vfb.neo4j.lmb2neoKB.lmb_query_tools import get_conn
import sys

vfb = map_iri(curie = 'vfb')
obo = map_iri(curie = 'obo')

## Requires ssh tunnel
c = get_conn(sys.argv[4], sys.argv[4])

edge_writer = kb_owl_edge_writer(endpoint= sys.argv[1], usr = sys.argv[2], pwd = sys.argv[3])
node_imp = node_importer(endpoint= sys.argv[1], usr = sys.argv[2], pwd = sys.argv[3])

cursor = c.cursor()

# Add uniqueness constraints:

print("*** Adding constraints and Indexes***")

node_imp.add_default_constraint_set(['Individual', 'VFB'])

# Add all individuals
# May need some regex escapes for some ind names?

cursor.execute("SELECT shortFormID, is_obsolete, label, " \
               "short_name, gene_name, idid, Name, Age, Putative_birth_time " \
               "FROM owl_individual oi " \
               "LEFT OUTER JOIN neuron n ON oi.uuid = n.uuid ")

i = 1
# statement_chunk = []
for d in cursor.fetchall():
    labels = ['Individual', 'VFB']
    IRI = vfb + d['shortFormID']
    ad = {}
    ad['short_form'] = d['shortFormID']
    ad['label'] = d['label']
    ad['is_obsolete'] = bool(int(d['is_obsolete']))
    synonyms = []
    xrefs = []
    if d['Putative_birth_time']: ad['comment'] = "Out"
    if d['Age']: ad['comment'] += "Age: %s" % str(d['Age'])
    if d['short_name']: synonyms.append(d['short_name'])
    if d['gene_name']: synonyms.append(d['gene_name'])
    if d['gene_name']: xrefs.append("FlyCircuit_gene_name:" + d['gene_name'])
    if d['idid']: xrefs.append("FlyCircuit_idid:" + str(d['idid']))
    if d['Name']: xrefs.append("FlyCircuit_name:" + d['Name'])  # Need to tweak this to => link to site.  But could do that in Cypher.
    if synonyms:  ad['synonyms'] = synonyms
    if synonyms:  ad['xrefs'] = xrefs
    node_imp.add_node(labels, IRI, ad)

        
print("*** Adding %d Individuals ***" % len(node_imp.statements))

node_imp.commit(chunk_length=5000, verbose=True)

# Import object properties:



cursor.execute("SELECT s.shortFormID AS subj_sfid, " \
               "s.label AS subj_label,  " \
               "r.shortFormID AS rel_sfid,  " \
               "r.label as rel_label,  " \
               "o.shortFormID AS obj_sfid,  " \
               "o.label AS obj_label,  " \
               "ront.baseURI AS rBase,  " \
               "ront.short_name as ront_name  " \
               "FROM owl_fact f  " \
               "JOIN owl_individual s ON (f.subject=s.id)  " \
               "JOIN owl_individual o ON (f.object=o.id)  " \
               "JOIN owl_objectProperty r ON (f.relation = r.id)  " \
               "JOIN ontology ront ON (r.ontology_id=ront.id)  ")  # Just bare triples.  Not pulling types. 

cypher_facts = []

properties = set([])
for d in cursor.fetchall():
    properties.add((d['rBase'] + d['rel_sfid'], d['ront_name']))
    edge_writer.add_fact(s = vfb + d['subj_sfid'],
                         r = d['rBase'] + d['rel_sfid'], 
                         o = vfb + d['obj_sfid'])

for p in properties:
    node_imp.add_node(labels = ['Property'], 
                      IRI = p[0],
                      attribute_dict = { 'label': [1]}
                      )
node_imp.commit()

print("*** Adding %d FACTs ***" % len(edge_writer.statements))

edge_writer.commit(chunk_length=2000, verbose=True)
edge_writer.test_edge_addition()



# Add all types

cursor.execute("SELECT oc.shortFormID AS claz, " \
               "oi.shortFormID AS ind, " \
               "oop.shortFormID AS rel_sfid, " \
               "oop.label AS rel_label, " \
               "ront.baseURI AS rBase, " \
               "ront.short_name AS ront_name, " \
               "cont.baseURI AS cbase " \
               "FROM owl_individual oi " \
               "JOIN individual_type it ON oi.id=it.individual_id " \
               "JOIN owl_type ot ON it.type_id=ot.id " \
               "JOIN owl_class oc ON ot.class = oc.id " \
               "JOIN owl_objectProperty oop ON ot.objectProperty=oop.id " \
               "JOIN ontology ront ON (oop.ontology_id=ront.id) " \
               "JOIN ontology cont ON (oc.ontology_id=cont.id)")

properties = set([])
    
for d in cursor.fetchall():
    if not d['rel_sfid']:
        edge_writer.add_named_type_ax(s = vfb + d['ind'], 
                                      o = d['cbase'] + d['claz']) # Should really be pulling base from SQL
    else:
        properties.add((d['rBase'] + d['rel_sfid'], d['ront_name']))
        edge_writer.add_anon_type_ax(s = vfb + d['ind'], 
                                     r = d['rBase'] + d['rel_sfid'],
                                     o = d['cbase'] + d['claz']) # Should really be pulling base from SQL

print( "*** Adding %d Types ***" % len(edge_writer.statements))
for p in properties:
    node_imp.add_node(labels = ['Property'], 
                      IRI = p[0],
                      attribute_dict = { 'label': [1]}
                      )
node_imp.commit()

#if edge_writer.check_proprties():
#    sys.exit("Mising properties in KB")  # Yeh, I know, should be using Try/Except....    

edge_writer.commit(chunk_length=2000, verbose=True) # chunk length of 5000 was causing requests connection to break
edge_writer.test_edge_addition()

## Link to datasets, adding id_in_source to edge, allowing rolling links.

cursor.execute("SELECT oi.shortFormID, ds.name, oi.id_in_source FROM owl_individual oi " \
               "JOIN data_source ds ON oi.source_id = ds.id " \
               "WHERE oi.shortFormID like 'VFBc\_%'")  # Only link channels to datasets.

statements = []

for d in cursor.fetchall():
    statements.append("MATCH (i:Individual { IRI : '%s' }), (ds:data_source { name : '%s'}) " \
                      "MERGE (i)-[:has_source  { id_in_source: '%s' }]->(ds)"
                      % (vfb + d['shortFormID'], d['name'], d['id_in_source']))  # Should make the id_in_source conditional

print("*** Adding %d dataset links ***" % len(statements))
node_imp.nc.commit_list_in_chunks(statements = statements, chunk_length = 2000, verbose=True) 
cursor.close()
c.close()
