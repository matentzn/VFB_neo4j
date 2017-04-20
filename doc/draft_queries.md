### Anatomical individual to cluster

~~~~~~~.cql

MATCH (:Individual { short_form :'VFB_00009700' })-[:Related { label : 'member_of' }]->
(cluster:Individual)-[:Related { label : 'has_exemplar' }]-(exemplar:Individual)
RETURN cluster, exemplar

--- Clusters returned are named "Cluster v.n" where v = clustering version, n = cluster id. 

MATCH (:Individual { short_form :'VFB_00009700' })-[:Related { label : 'member_of' }]->
(cluster:Individual)-[:Related { label : 'member_of' }]-(member:Individual)
RETURN member
~~~~~~~~~~



### Anatomical results page query

The IN clause should be populated by an appropriate number of results.
1 query (20 results?) => first page 
Second query in background => complete results table

QUERY STATUS: TESTED, WORKS

```cql
MATCH (n:VFB:Class) WHERE n.short_form IN 
['FBbt_00111464', 'FBbt_00007422', 'FBbt_00007225', 'FBbt_00100477'] 
WITH n 
OPTIONAL MATCH (n)<-[:SUBCLASSOF|INSTANCEOF*1..4]-(a:Individual)
<-[:Related { label: 'depicts'}]-(c:Individual)
<-[:Related { label: 'has_signal_channel'}]-(image:Individual) with n,a, image
MATCH (n)-[r:has_reference]->(p:pub)
RETURN n.label AS class_label, n.description as class_def, n.short_form AS class_id, n.synonym,
COLLECT (DISTINCT { FlyBase: p.FlyBase, miniref:  p.miniref}) AS pubs,  
COLLECT (DISTINCT { anat_ind_name: a.label, image_id: image.short_form}) AS inds

```

### Generating trees for a given template.

The following query finds shortest path from anatomical Individual to root term for graph, ie. the leaf nodes are all Individuals.

Possible drawback:  There is no filter to ensure that only neuropil/tract nodes show up in the partonomy.  Some possibility of odd results with cells turning up in paths.

Query Status: Tested.  Works.

~~~~~~~~.cql

MATCH (t:Individual { label : 'JFRC2_template'})<-[:Related { label : 'has_background_channel' }]
-(image:Individual)-[:Related { label : 'has_signal_channel' }]-(c:Individual)
-[:Related { label : 'has specified output' }]->(p:Class { label : 'computer graphic' }), 
(c)-[:Related { label : 'depicts' } ]->(a:Individual) 
WITH  COLLECT (a.short_form) as tree_nodes
MATCH p=allShortestPaths((root:Class { label : 'adult brain'
})<-[:SUBCLASSOF|part_of|INSTANCEOF*..]-(anat:Individual)) WHERE anat.short_form IN tree_nodes
RETURN p

~~~~~~~~~~

#### Alternative, returning graph returning classes only

Query Status: Tested.  Works.

~~~~~~~~~~~.cql

MATCH (t:Individual { label : 'JFRC2_template'})<-[:Related { label : 'has_background_channel' }]
-(image:Individual)-[:Related { label : 'has_signal_channel' }]-(c:Individual)
-[:Related { label : 'has specified output' }]->(p:Class { label : 'computer graphic' }),
(c)-[:Related { label : 'depicts' } ]->(a:Individual)-[:INSTANCEOF]->(ac:Class) 
WITH  COLLECT (ac.short_form) as tree_nodes
MATCH p=allShortestPaths((root:Class { label : 'adult brain'
})<-[:SUBCLASSOF|part_of*..]-(anat:Class)) WHERE anat.short_form IN tree_nodes
RETURN p

~~~~~~~~~~~

#### Alternative, returning class graph + separate map from class to domain.

Query Status: Tested.  Works

~~~~~~~~~~~.cql

MATCH (t:Individual { label : 'JFRC2_template'})<-[:Related { label : 'has_background_channel' }]
-(image:Individual)-[:Related { label : 'has_signal_channel' }]-(c:Individual)
-[:Related { label : 'has specified output' }]->(p:Class { label : 'computer graphic' }),
(c)-[:Related { label : 'depicts' } ]->(a:Individual)-[:INSTANCEOF]->(ac:Class) 
WITH  COLLECT (ac.short_form) as tree_nodes, 
COLLECT (DISTINCT{ image: image.short_form, anat_ind: a.short_form, type: ac.short_form}) AS domain_map
MATCH p=allShortestPaths((root:Class { label : 'adult brain'
})<-[:SUBCLASSOF|part_of*..]-(anat:Class)) WHERE anat.short_form IN tree_nodes
RETURN p, domain_map

~~~~~~~~~~~~~
