from owlready2 import *

onto_path.append('neo4j-onto-loader')
onto = get_ontology('http://purl.obolibrary.org/obo/foodon.owl').load()

obo = get_namespace('http://purl.obolibrary.org/obo/')

# #classes = list(onto.classes())
# props = list(onto.disjoint_properties())
#obj_prop = list(onto.object_properties())

# for i in props:
#      print("{}, {}".format(i,i.label))
#
# print("\n\n")

# for i in obj_prop:
#      print("{}, {}".format(i,i.label))
#
# print("\n\n")

#FOODON_03510023
print(obo.FOODON_03412090.label)
#print(obo.FOODON_03412090.hasSynonym)
print(obo.FOODON_03412090['has narrow synonym'])
#print(obo.FOODON_03412090.hasExactSynonym)
print(obo.FOODON_03412090.is_a)
print(obo.FOODON_03412090.hasDbXref)
print(obo.FOODON_03412090.IAO_0000115)
print(obo.FOODON_03412090.comment)

# print(obo.FOODON_03302108.label)
# print(obo.FOODON_03302108.comment)
# print(obo.FOODON_00003257.hasBroadSynonym)