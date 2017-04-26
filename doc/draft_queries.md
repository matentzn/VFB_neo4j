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



### Anatomical class results page query

QUERY STATUS: TESTED, WORKS

TBA: Needs to pull back template, but waiting on tweaks to schema.

```cql
MATCH (n:VFB:Class) WHERE n.short_form IN 
['FBbt_00111464', 'FBbt_00007422', 'FBbt_00007225', 'FBbt_00100477'] 
WITH n 
OPTIONAL MATCH (n)<-[:SUBCLASSOF|INSTANCEOF*1..4]-(a:Individual)
<-[:Related { label: 'depicts'}]-(c:Individual)
<-[:Related { label: 'has_signal_channel'}]-(image:Individual) 
WITH n, COLLECT (DISTINCT { anat_ind_name: a.label, image_id: image.short_form}) AS inds
MATCH (n)-[r:has_reference]->(p:pub)
WITH n, inds, COLLECT (DISTINCT { FlyBase: p.FlyBase, miniref:  p.miniref}) AS pubs
RETURN n.label AS class_label, n.description as class_def, n.short_form AS class_id, n.synonym,
pubs, inds[1..6] AS inds
```

Mapping to columns: 

![image](https://cloud.githubusercontent.com/assets/112839/25243485/141d09c6-25f5-11e7-9b49-bdda3f0154db.png)

We don't need a whole column for controls. Pubs should be hyperlinked microrefs (now available in prod).
synonyms - In a new column or in name column?


### Anatomical Individuals results page query

```cql
MATCH (n:VFB:Individual) WHERE n.short_form IN 
['VFB_00001000', 'VFB_00001002', 'VFB_00001003'] 
WITH n 
MATCH (image:Individual)-[:Related { label: 'has_signal_channel'}]->(channel:Individual)-[:Related { label: 'depicts'}]->(n)-[:INSTANCEOF]->(typ:Class)
RETURN n.label AS anat_ind_label, n.description as anat_ind_def, n.short_form AS anat_ind_id, n.synonym AS anat_ind_syn,  
COLLECT (DISTINCT { type_label: typ.label, type_id: typ.short_form}) AS types, image.short_form as image_id
```

TODO: Extend to templates and source, pub(s) once new schema is in place.

### Cluster queries

VFB1.5:

![image](https://cloud.githubusercontent.com/assets/112839/25244911/726ad364-25fa-11e7-93bc-1b118c601e0e.png)


Exemplar name	Exemplar definition	Exemplar source	Exemplar preview	Members of cluster

Open 63 in viewer
List individual members

New columns: 
* Columns: 
  * NBLAST Cluster
    * display name + thumbnail + link to terminfo; 
  * Types 
  * Q: List individual members
  
  
```cql
MATCH (channel:Individual)-[:depicts]->(c:Cluster)<-[:Related { label: 'exemplar_of' }]-(i:Individual)
-[:INSTANCEOF]->(clz:Class) 
WHERE not (clz.label ="neuron") 
RETURN DISTINCT i.label, c.label, clz.label, channel.IRI
```
What's missing:

1. Links to channels from clusters (=> thumbnail URL).
2. Standard linkout pattern for cluster to LMB site for linkout.
  
  

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
