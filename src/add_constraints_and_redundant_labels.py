 #!/usr/bin/env python
import requests
import sys

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]


## Add constraints

def commit_list(statements, base_uri, usr, pwd):
    statements=[]
    for c in constraints:
        s.append({'statament': x})
    payload = {'statements': statements}
    return requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))

    
constraints = ['CREATE CONSTRAINT ON (c:Class) ASSERT c.short_form IS UNIQUE',
                   'CREATE CONSTRAINT ON (c:Individual) ASSERT c.short_form IS UNIQUE']

commit_list(constraints, base_uri, usr, pwd)


## Denormalise - adding labels for major categories:

# probably better to do by ID...
# A more flexible structure would use lists in values to allow labels from unions
label_types = {
   'Neuron': 'neuron',
   'Tract': 'synaptic neuropil tract',
   'Synaptic_neuropil': 'synaptic neuropil',
   'Clone': 'neuroblast lineage clone'
   }

label_additions = []
for l,t in label_type.items():
   label_additions.append("MATCH (n:Class)-[r:SUBCLASSOF*]->(n2:Class) WHERE n2.label = '%s' SET n:%s" % (l, t))

commit_list(label_additions, base_uri, usr, pwd)

