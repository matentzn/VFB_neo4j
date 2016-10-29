# VFB\_neo\_KB spec

## General description:

\1. A persistence store for image annotation.

*Image annotation consists of:*

* OWL anatomical individuals annotated with type expressions using RO relations and ontology classes / FlyBase features (modelled as classes).

* Image individuals annotated using FBbi and a bespoke extension to FOAF:Image

* Triples link anatomical individuals to image individuals

\2. A store for connectivity data - dumped from third party sources, mapping to image annotation.

*There are two interpretations of connectivity data:*

* Crude Triples A synapsed_to B - with synapse counts on edges.

* Individual synapse partonomy represented (3 individuals per synapse, typed using GO.


## Requirements

* Ontology terms and FlyBase features used in annotation must be update-able via CI.  

* The IDs of derived ontology classes (e.g. expression patterns) must be preserved during updates.

* A subset of content must be easy to convert to OWL using simple, standard patterns.  Content with OWL semantics is be specified as a set of Cypher pattern match statements mapping triples from Neo to OWL (link)
  
* There are two types of content not intended for conversion to OWL.
   * Content whose semantics is intended to be only in neo4J (e.g. links to data sources and licenses)
   * Content whose semantic interpretation is in OWL, but which, for pragmatic reasons, is not converted.
      
* It must be possible to generate OWL files corresponding to individual datasets.
   
   

# VFB\_neo\_prod


# VFB\_Solr\_prod

A SOLR endpoint to drive autocomplete on VFB.

Official names and synonyms should be indexed for all of these entities:

* Ontology terms (via OLS loader)
* Anatomical individuals (via OLS loader)
* FlyBase features  ( ? Do we really want all of these? )
* Entities derived from FlyBase:  Genotypes, Expression patterns.


Ontology terms and anatomical individuals + 

FlyBase features and expression patterns + synonyms

# VFB\_OWL\_prod

An OWL reasoner endpoint (Currently using AberOWL)

Required for:
Anatomy queries 


# Expression pattern spec


* Expression patterns are derived from FlyBase features, with which they have a 1:1 relationship.  (TBA - link to semantic spec.)

* Expression pattern IDs must be stable

* Classification of expression patterns & expression pattern fragments should work via OWL reasoning:

Class: 'expression pattern of X' EquivalentTo 'expression pattern that expresses some X'

'expression pattern of X in Y': 

GCI: cell that expresses some X SubClassOf part_of some 
	
	Expression pattern of X in Y:  the part of  
	


