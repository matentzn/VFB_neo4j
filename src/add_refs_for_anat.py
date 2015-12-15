from owltools.graph import OWLGraphWrapper
from owl2pdm_tools import ont_manager
from uk.ac.ebi.brain.core import Brain  # May be better to avoid Brain in this case...
import requests
import sys
import re
import json

"""Add typing via anonymous class expressions from OWL file.
Requires uniqueness constraint on individual & class short_form_id."""

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

vfb = Brain()
vfb.learn("/repos/VFB_owl/src/owl/fbbt_vfb_ind_prc_nr.owl")
vom = ont_manager(vfb.getOntology())
ogw = OWLGraphWrapper(vfb.getOntology())


fbbt_classes = vfb.getSubClasses("FBbt_10000000", 0) # FBbt root

#Requires uniqueness constraint on pub key fields from start

for sfid in fbbt_classes:
    dbxrefs = ogw.getDefXref(vom.bi_sfp.getEntity(sfid))
    statements = []
    if not dbxrefs:
        continue
    for xref in dbxrefs:
        if re.match("FlyBase:FBrf\d{7}", xref):
            fbrf = xref.split(':')[1]
            statements.append({ 'statement': "MATCH (a:Class { short_form : '%s' }) MERGE (a)-[:has_reference]->(:pub { FBrf : '%s' })"
                             % (sfid, fbrf) })
    if statements:
        payload = {'statements': statements}
        rels = requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))

        
        
