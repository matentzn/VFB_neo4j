#  VFB neo4J schema notes

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

(Class { label : 'C1'}  ) -[Related {label : 'R'}]->(Class { label 'C2' } )
(Class { label : 'C2'}  ) -[SUBCLASSOF]->(Class { label 'C3' } )
(Individual { label : 'I1'}  ) -[INSTANCEOF]->(Class { label 'C1' } )

Extension => more graphy expressiveness

Individual: I1
   Type: R some C1 
Fact:  R I2

Class: C4
   SubClassOf R value I3

=> 

(Individual { label : 'I1'}  ) -[Related {label : 'R'}]->(Class { label 'C1' } ) #  This feels slightly hacky, but so does using INSTANCEOF with a varierty of edge labels as for Related.
(Individual { label : 'I1'}  ) -[Related {label : 'R'}]->(Individual { label 'I2' } )  
(Class { label : 'C4'}  ) -[Related {label : 'R'}]->(Individual { label 'I3' } )

Any more complex axioms are not stored in the graph.

## Denormalisig the OLS graph for improved querying:

The lack of edges named for relations causes a problem for querying:  It is not possible to specify a set of edges in a pattern match by their attributes.  To compensate for this, we run a script that duplicates all the 'related_to' edges - making named edges in their place.  

This allows, for example, the following neuron location graphing genertor:

~~~~~.cql


MATCH (fu)-[r1:overlaps|has_synaptic_terminal_in|has_postsynaptic_terminal_in|has_presynaptic_terminal_in]
->(np:Class)-[r2:part_of*]->(np2:Synaptic_neuropil) 
WHERE fu.label = 'lamina monopolar neuron L1' 
RETURN fu, r1, r2, np, np2;

~~~~~~~~~

![image](https://cloud.githubusercontent.com/assets/112839/11816078/a753fd36-a346-11e5-8b71-5d054ca5d452.png)


### References

References are entities in the schema.  Any other entity (anatomy may have a has_reference relation to reference)

e.g. 



Question: Are there some cases where enitites need to be spec'd as attributes?

Synonyms?



