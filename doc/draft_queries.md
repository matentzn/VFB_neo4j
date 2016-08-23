
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

~~~~~~~~~~

Alternative, returning graph returning classes only

Query Status: Tested.  Works.

~~~~~~~~~~~.cql

MATCH (t:Individual { label : 'JFRC2_template'})<-[:Related { label : 'has_background_channel' }]-(image:Individual)-[:Related { label : 'has_signal_channel' }]-(c:Individual)-[:Related { label : 'has specified output' }]->(p:Class { label : 'computer graphic' }), (c)-[:Related { label : 'depicts' } ]->(a:Individual)-[:INSTANCEOF]->(ac:Class) 
WITH  COLLECT (ac.short_form) as tree_nodes
MATCH p=allShortestPaths((root:Class { label : 'adult brain'
})<-[:SUBCLASSOF|part_of*..]-(anat:Class)) WHERE anat.short_form IN tree_nodes

~~~~~~~~~~~

Alternative, returning class graph + separate map from class to domain.

Query Status: Tested.  Works

~~~~~~~~~~~.cql

MATCH (t:Individual { label : 'JFRC2_template'})<-[:Related { label : 'has_background_channel' }]-(image:Individual)-[:Related { label : 'has_signal_channel' }]-(c:Individual)-[:Related { label : 'has specified output' }]->(p:Class { label : 'computer graphic' }), (c)-[:Related { label : 'depicts' } ]->(a:Individual)-[:INSTANCEOF]->(ac:Class:) 
WITH  COLLECT (ac.short_form) as tree_nodes, COLLECT (DISTINCT{ image: image.short_form, anat_ind: a.short_form, type: ac.short_form}) AS domain_map
MATCH p=allShortestPaths((root:Class { label : 'adult brain'
})<-[:SUBCLASSOF|part_of*..]-(anat:Class)) WHERE anat.short_form IN tree_nodes
RETURN p, domain_map

~~~~~~~~~~~~~
