#  VFB neo4J schema notes

STATUS: DRAFT

## OLS representation of OWL
OLS Neo4J representation of OWL currenly handles type assertions linking individuals to named classes (via instanceOf).  Would it be possible to have type expressions of the form "I type: R some C" translated as relationships [I -R- C] in the Neo4J graph?  Presumably an identical pattern is used to convert SubClassOf axioms to part_of etc relationships in OLS Neo4J.  


Default OLS OWL translation:

Class: C1
   SubClassOf: R some C2

Class: C2 
   SubClassOf: C3

Individual: I1
  Type: C1

=> 

~~~~~~~.cql

(Class { label : 'C1'}  ) -[Related {label : 'R'}]->(Class { label 'C2' } )
(Class { label : 'C2'}  ) -[SUBCLASSOF]->(Class { label 'C3' } )
(Individual { label : 'I1'}  ) -[INSTANCEOF]->(Class { label 'C1' } )

~~~~~~~~

Extensions to OLS representation:

Individual: I1
   Type: R some C1 
Fact:  R I2

Class: C4
   SubClassOf R value I3

=> 

~~~~~~~.cql
(:Individual { label : 'I1'}  ) -[:Related {label : 'R'}]->(:Class { label 'C1' } )
(:Individual { label : 'I1'}  ) -[:Related {label : 'R'}]->(:Individual { label 'I2' } )  
(:Class { label : 'C4'}  ) -[:Related {label : 'R'}]->(Individual { label 'I3' } )
~~~~~~~~~

Any more complex axioms are not stored in the graph.

All labels/shortForm IDs on Related edges corrspond to a Property node.

## Denormalising the OLS graph for improved querying:

The lack of edges named for relations causes a problem for querying:  It is not possible to specify a set of edges in a pattern match by their attributes.  To compensate for this, we run a script that duplicates all the 'related_to' edges - making named edges in their place.  

This allows, for example, the following neuron location graph generator:

~~~~~.cql

MATCH (fu)-[r1:overlaps|has_synaptic_terminal_in|has_postsynaptic_terminal_in|has_presynaptic_terminal_in]
->(np:Class)-[r2:part_of*]->(np2:Synaptic_neuropil) 
WHERE fu.label = 'lamina monopolar neuron L1' 
RETURN fu, r1, r2, np, np2;

~~~~~~~~~

![image](https://cloud.githubusercontent.com/assets/112839/11816078/a753fd36-a346-11e5-8b71-5d054ca5d452.png)

### Neo4J native content destined for conversion to OWL

We want to allow for translation of some content in neo4J to OWL.  For this reason, it is important that all content destined for translation be added using the OWL translation defined above.  Edges can then be subsequently duplicated as named edges for query purposes.  Any other attributes should be copied over at this time.

### References and synonyms on ontology classes & individuals

~~~~~~.cql

(:class:anatomy)-[:has_reference { type: def }] -> (:pub)
(:class:anatomy)-[:has_reference {type: 'synonym'; label: 'fu' ; scope: 'exact' }] -> (:pub)

~~~~~~~

#### Use case queries

1. TermInfo query on a term
Find all definition references (miniref + hyperlink)
Find all synonyms, their scopes and references (miniref + hyperlink)

~~~~~.cql
MATCH (a:class:anatomy)-[hr:has\_reference { type: def }] -> (p:pub) where a.short_form = '%s' RETURN a, hr, p;
~~~~~

2. Find all data directly linked to a specified reference:
 - Used for definition or synonym for anatomical entity
 - Reference for expression, phenotypes

~~~~~~.cql
MATCH (a:class:anatomy)-[hr:has\_reference { type: def }] -> (p:pub) where p.short_form = '%s' RETURN a, hr, p;
~~~~~~

Note - this means some denormalisation of synonym info:  if there are multiple references for a single synonym then the same synonym will live on multiple edges.  However the complexity of the synonym field means that it can't fit into the simple data types available in node attributes.  Adding synonyms as nodes in their own right would add needless complexity.


### Expression data

~~~~ .cql
(ep:expression pattern { label :  "expression pattern of X", short_form  : "VFB…." }) 

(ep)-[:instanceOf]->(:Individual:Anatomy: { short_form : "VFB\_1234567" })

(ep)-[expresses]->(:genetic\_feature {  label : 'p{X}', short\_form : "FBbi..." })

(ep)-[:overlaps { short\_form : 'RO\_…',  pubs: ["FBrf12343567"]}]
->(as:Class:Anatomy { "label" :  'lateral horn  - from S-x to S-y', short_form : 'FBex...', assay: ''})

(as)-[SubClassOf]->(:Anatomy { label:  'lateral horn', short\_form: "FBbt_...." })

(sr)-[during]->(:stage { label: 'stage x', short\_form: 'FBdv_12345678' }
(sr)-[during]->(:stage { label: 'stage y', short\_form: 'FBdv_22345678' }
...
~~~~~

(Note - a cypher query can be used to fill in intermediate stages not recorded in FlyBase).

### Anatomical Phenotype data

(NEEDS WORK!)

~~~~~~.cql

(g:genotype { label: 'fu[1]/fu[2]')-[:has_part]->(:allele { label : 'fu[1]'})
(g:genotype { label: 'fu[1]/fu[2]')-[:has_part]->(:allele { label : 'fu[2]'})

(g)-[:phenotype\_expressed\_in { pub: 'FBrf...' }, description: 'ipsum lorerm...' ]->(ap:Class:Anatomy { label: 'lateral horn during adult stage' })
(ap)-[:SubClassOf { label: 'lateral horn', short_form: 'FBbt_...' }
(ap)-[:during { label: 'adult stage', short_form: 'FBdv_....' }]

~~~~~~~~

