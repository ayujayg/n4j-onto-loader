OWL ontology to Neo4j formatter

Steps
1. Load .owl ontology into neo4j using Neo4j's
>n10s.onto.import.fetch($filepath)
> 
>n10s.rdf.import.fetch($filepath)

2. Connect main.py to the loaded Neo4j graph from step 1 and run. note the .pkl files used to store the nodes and relationships
3. Initialize a separate empty Neo4j graph
4. In rewrite.py, connect to the new graph and run, using the dataframes from step 2. 
5. The contents of the original Neo4j Graph should be formatted with reduced clutter