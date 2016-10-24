from uk.ac.ebi.vfb.neo4j.tools import neo4j_connect 

nc = neo4j_connect('http://localhost:7474', 'neo4j', 'neo4j')

nc.commit_list("CREATE (t:Test { fu: 'bar' })"):
  
