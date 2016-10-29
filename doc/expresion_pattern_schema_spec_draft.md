
The regional part problem

membrane: A lipid bilayer and all the embedded proteins in it.  

'whole membrane'
	SubClassOf membrane


'membrane region'
	EquivalentTo: membrane that part_of some membrane
	disjointWith whole membrane
	
	
'expression pattern': The mereological sum of all cells in some specified anatomical structure^ that express some specified gene or transgene.  

	'complete expression pattern':  The mereological sum of all cells in an organism (at all stages) that express some specified gene or transgene. 

	regional expression pattern
		EquivalentTo: expression pattern that part_of* some expression pattern
		EquivalentTo: expression pattern that part_of some complete expression pattern


Patterns: 


	expression pattern of X: 
	
	classes: expression pattern
	
	vars
	
	
	def: The mereological sum of all cells in some specified anatomical structure^ that express X

	regional expression pattern of X
		EquivalentTo: expression pattern that part_of* some expression pattern
		EquivalentTo: expression pattern that part_of some complete expression pattern
		Disjoint with complete expression pattern of X
	
	complete expression pattern of X - The mereological sum of all cells in some organism (at all stages) that express X
	

^ Anatomical structure includes organism	
* part of here is improper.
	
	