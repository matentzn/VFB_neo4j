[![Build Status](https://travis-ci.org/VirtualFlyBrain/VFB_neo4j.svg?branch=master)](https://travis-ci.org/VirtualFlyBrain/VFB_neo4j)


# VFB_neo4j

Neo4J server:

* VFB\_neo_KB: The future home of the VFB knowledgeBase.  It is currently populated with individuals representing anatomy, channels and images + FACTs relating these.  Type assertions on channels and images are present.  Type assertions on anatomical individuals have yet to be moved from the LMB SQL DB.  This DB lives on bocian.

* VFB\_neo_prod: production neo4J server.  This DB is built from a number of sources: OWL loaded via the OLS loader + some side loading from OWL via Jython scripts (These should be ditched if/when OLS add support for importing the relevant axioms); Data directly imported from FlyBase; Data imported from the LMB DB (The plan is to turn this off ASAP and switch to pulling from neo4J KB.  This DB lives on blanik.

* VFB\_neo_dev: development version of the production server
