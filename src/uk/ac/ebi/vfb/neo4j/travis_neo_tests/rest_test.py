from uk.ac.ebi.vfb.neo4j.tools import neo4j_connect
import sys

nc = neo4j_connect('http://localhost:7474', 'neo4j', 'neo4j')

if not nc.commit_list("CREATE (t:Test { fu: 'bar' })"):
  sys.exit('1')
  
  
