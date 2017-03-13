'''
Created on 13 Aug 2016

@author: davidos
'''
# A temporary expedient until import from OWL fixed, or we move to a native neo4J implementation of KB

from ..KB_tools import kb_owl_edge_writer
from ...curie_tools import map_iri
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect
from .lmb_query_tools import get_conn
import sys

vfb = map_iri(curie = 'vfb')
obo = map_iri(curie = 'obo')

## Requires ssh tunnel
c = get_conn('flycircuit', 'flycircuit')

nc = neo4j_connect(base_uri = sys.argv[1], usr = sys.argv[2], pwd = sys.argv[3])
edge_writer = kb_owl_edge_writer(endpoint= sys.argv[1], usr = sys.argv[2], pwd = sys.argv[3])
cursor = c.cursor()

# Add uniqueness constratints:

cypher_constraints = ["CREATE CONSTRAINT ON (i:Individual) ASSERT i.short_form IS UNIQUE",
                      "CREATE CONSTRAINT ON (i:Individual) ASSERT i.iri IS UNIQUE",
#                      "CREATE INDEX ON :Individual(ontology_name)",
                      "CREATE INDEX ON :Individual(label)"]

nc.commit_list(cypher_constraints)

# Add all individuals
# May need some regex escapes for some ind names?

inds_2_add = []
cursor.execute("SELECT shortFormID, is_obsolete, label, short_name, gene_name, idid, Name, Age, Putative_birth_time " \
                "FROM owl_individual oi LEFT OUTER JOIN neuron n ON oi.uuid = n.uuid " \
               "FROM owl_individual")

for d in cursor.fetchall():
    statement = "MERGE (i:Individual:VFB { IRI : '%s' } ) "  % vfb + d['short_form']
    statement += "SET i.label = '%s' " % d['label']
    statement += "SET i.is_obsolete = '%s'"  % bool(d['is_obsolete'])
    synonyms = []
    xrefs = []
    if d['short_name']: synonyms.append(d['short_name'])
    if d['gene_name']: synonyms.append(d['gene_name'])
    if d['gene_name']: xrefs.append("FlyCircuit_gene_name:" + d['gene_name'])
    if d['idid']: xrefs.append("FlyCircuit_idid:" + d['idid'])
    if d['Name']: xrefs.append("FlyCircuit_name:" + d['Name'])
    if synonyms: statement += "SET i.synonyms = %s"  % str(synonyms)
    if xrefs: statement += "SET i.xrefs = %s"  % str(synonyms)
    inds_2_add.append(statement)
    
nc.commit_list(inds_2_add)

## Add all facts

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


for d in cursor.fetchall():
    edge_writer.add_fact(s = vfb + d['subj_sfid'],
                         r = d['rBase'] + d['rel_sfid'], 
                         o = vfb + d['obj_sfid'])


edge_writer.commit()

# Add all types

cursor.execute("SELECT oc.shortFormID AS claz, " \
               "oi.shortFormID AS ind, " \
               "oop.shortFormID AS rel_sfid, " \
               "oop.label AS rel_label, " \
               "ront.baseURI AS rBase, " \
               "ront.short_name AS ront_name " \
               "FROM owl_individual oi " \
               "JOIN individual_type it ON oi.id=it.individual_id " \
               "JOIN owl_type ot ON it.type_id=ot.id " \
               "JOIN owl_class oc ON ot.class = oc.id " \
               "JOIN owl_objectProperty oop ON ot.objectProperty=oop.id " \
               "JOIN ontology ront ON (oop.ontology_id=ront.id)")
    
for d in cursor.fetchall():
    if not d['rel_sfid']:
        edge_writer.add_named_type_ax(s = vfb + d['ind'], 
                                      o = obo + d['claz']) # Should really be pulling base from SQL
    else:
        edge_writer.add_anon_type_ax(s = vfb + d['ind'], 
                                     r = d['rBase'] + d['rel_sfid'],
                                      o = obo + d['claz']) # Should really be pulling base from SQL
        
    
edge_writer.commit()

## Link to datasets, adding id_in_source to edge, allowing rolling links.

cursor.execute("SELECT oi.shortFormID, ds.name, oi.id_in_source FROM owl_individual oi " \
               "JOIN data_source ds ON oi.source_id = ds.id " \
               "WHERE oi.shortFormID like 'VFBc\_%'")  # Only link channels to datasets.

statements = []

for d in cursor.fetchall():
    statements.append("MATCH (c:Individual { short_form : '%s' }), (ds:data_source { name : '%s'})" \
                      "MERGE (c)-[:has_source  { id_in_source: '%s' }]->(ds)" 
                      % (d['shortFormID'], d['name']), d['id_in_source'])  # Should make the id_in_source conditional

nc.commit_list_in_chunks(statements, chunk_length = 1000)
cursor.close()
c.close()
