#!/usr/bin/env python3
import sys
from uk.ac.ebi.vfb.neo4j.tools import neo4j_connect

nc = neo4j_connect(base_uri = sys.argv[1], usr = sys.argv[2], pwd = sys.argv[3])

# Some AP deletions required for uniqueness constraints.  Needed due to quirks of OLS import.

deletions = ["MATCH (n:VFB { short_form: 'deprecated' })-[r]-(m) DELETE r, n;"]
nc.commit_list(deletions)

## Run tests prior to adding uniqueness constraints:

#test = ["MATCH (n:VFB) with n.short_form as prop, collect(n) as nodelist, count(*) as count where count > 1 return prop, nodelist, count"]

#test_results = nc.commit_list(test)

# Some processing
# If test results have contents then die.

## Add constraints


# Commenting for now. constraints = ['CREATE CONSTRAINT ON (c:VFB) ASSERT c.short_form IS UNIQUE', 
#               'CREATE CONSTRAINT ON (c:VFB) ASSERT c.short_form IS UNIQUE']
# nc.commit_list(constraints)
# 
# Should really give up if constraints fail.

### Cypher query to find dups.
# "MATCH (n:VFB)
# WITH n.short_form AS prop, collect(n) AS nodelist, count(*) AS COUNT
# WHERE count > 1
# RETURN prop, nodelist, count;"

## Denormalise - adding labels for major categories:

# probably better to do by ID...
# A more flexible structure would use lists in values to allow labels from unions
# Also add label type for FlyBase feature?

label_types = {
   'Neuron': 'neuron',
   'Tract': 'synaptic neuropil tract',
   'Synaptic_neuropil': 'synaptic neuropil',
   'Synaptic_neuropil_domain': 'synaptic neuropil domain',
   'Synaptic_neuropil_subdomain': 'synaptic neuropil subdomain',
   'Synaptic_neuropil_block': 'synaptic neuropil block',   
   'Clone': 'neuroblast lineage clone'
   }

label_additions = []
for k,v in label_types.items():
    label_additions.append("MATCH (n:Class)-[r:SUBCLASSOF*]->(n2:Class) WHERE n2.label = '%s' SET n:%s" % (v, k))

nc.commit_list(label_additions)

