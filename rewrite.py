import py2neo as p2
import pandas as pd
from py2neo import NodeMatcher
import logging
import random

gs = p2.Graph("bolt://localhost:7687", auth=("neo4j", "aurpon"), user="neo4j", password="aurpon")

properties_df = pd.read_pickle("PO_df.pkl")
subclass_df = pd.read_pickle("SCO_df.pkl")
hasvalues_df = pd.read_pickle("HV_df.pkl")

gs.delete_all()
matcher = NodeMatcher(gs)

rels = []
nodes = []
uri_id_map = {}

node_id = 0

bnode_id_map = {}


def get_bnode_id(id):
    eval = "match (a:bnode {bnode_id: " + str(id) + "}) return a limit 1"
    node = gs.evaluate(eval)
    return node.identity


def get_node_id(name):
    eval = "match (a:" + name + ") return a limit 1"
    node = gs.evaluate(eval)
    return node.identity


def MEMBER_OF(tx, n1, n2):
    MEMBER_OF = p2.Relationship(n1, "MEMBER OF", n2)
    rels.append(MEMBER_OF)
    tx.create(MEMBER_OF)


def on_property(tx, n1, r, n2, rtype):
    onProperty = p2.Relationship(n1, r, n2)
    if rtype == 0:
        onProperty['restriction'] = "someValuesFrom"
    elif rtype == 1:
        onProperty['restriction'] = "onlyValuesFrom"
    elif rtype == 2:
        onProperty['restriction'] = 'hasValue'
    rels.append(onProperty)
    tx.create(onProperty)


def SUBCLASS_OF(tx, n1, n2):
    SUBCLASS_OF = p2.Relationship(n1, "SCO", n2)
    rels.append(SUBCLASS_OF)
    tx.create(SUBCLASS_OF)


def subclass_of_rel(tx, n1, n2):
    subclass_of = p2.Relationship(n1, "isSubclassOf", n2)
    rels.append(subclass_of)
    tx.create(subclass_of)


for index, row in subclass_df.iterrows():
    tx = gs.begin()
    n = p2.Node(row['n.n4sch__name'], name=row['n.n4sch__name'], altlabel=row['n.skos__prefLabel'],
                uri=row['n.uri'])
    n.add_label("owl_Class")
    tx.merge(n, row['n.n4sch__name'], 'uri')
    nodes.append(n)

    if row['m.uri'] not in uri_id_map:
        m = p2.Node(row['m.n4sch__name'], name=row['m.n4sch__name'], altlabel=row['m.skos__prefLabel'],
                    uri=row['m.uri'])
        m.add_label("owl_Class")
        tx.merge(m, row['m.n4sch__name'], 'uri')
        subclass_of_rel(tx=tx, n1=n, n2=m)
    else:
        m = matcher.get(identity=uri_id_map[row['m.uri']])
        subclass_of_rel(tx=tx, n1=n, n2=m)

    tx.commit()
    uri_id_map[row['n.uri']] = get_node_id(row['n.n4sch__name'])
    if row['m.uri'] not in uri_id_map:
        uri_id_map[row['m.uri']] = get_node_id(row['m.n4sch__name'])

properties_grouped = properties_df.groupby('n.n4sch__name')

node_id = random.randint(15001, 20000)
union_id = random.randint(10001, 15000)

for name, group in properties_grouped:
    first_iter = False
    tx = gs.begin()

    U = p2.Node("bnode", bnode_id=union_id)
    U.add_label("UnionNode")
    tx.merge(U, "bnode", 'bnode_id')

    I = p2.Node("bnode", bnode_id=union_id + 1)
    I.add_label("IntersectionNode")
    tx.merge(I, "bnode", 'bnode_id')

    a = p2.Node("bnode", bnode_id=node_id)
    a.add_label("TransitiveNode")
    tx.merge(a, "bnode", 'bnode_id')

    tx.commit()

    bnode_id_map[union_id] = get_bnode_id(union_id)
    bnode_id_map[union_id + 1] = get_bnode_id(union_id + 1)
    bnode_id_map[node_id] = get_bnode_id(node_id)
    node_id += 1
    union_id += 2

    for index, row in group.iterrows():
        tx = gs.begin()

        if row['n.uri'] not in uri_id_map:
            n = p2.Node(row['n.n4sch__name'], name=row['n.n4sch__name'], altlabel=row['n.n4sch__label'],
                        uri=row['n.uri'])
            tx.merge(n, row['n.n4sch__name'], 'uri')
        elif row['x.uri'] not in uri_id_map:
            m = p2.Node(row['x.n4sch__name'], name=row['x.n4sch__name'], altlabel=row['x.n4sch__label'],
                        uri=row['x.uri'])
            tx.merge(m, row['x.n4sch__name'], 'uri')
        else:
            n = matcher.get(uri_id_map[row['n.uri']])
            m = matcher.get(uri_id_map[row['x.uri']])

            if not first_iter:
                SUBCLASS_OF(tx, n1=n, n2=a)
                on_property(tx, n1=a, r=row['p.n4sch__name'], n2=U, rtype=1)
                on_property(tx, n1=a, r=row['p.n4sch__name'], n2=I, rtype=0)
                first_iter = True

            MEMBER_OF(tx, m, U)
            MEMBER_OF(tx, m, I)

            tx.commit()

for index, row in hasvalues_df.iterrows():
    tx = gs.begin()
    n = matcher.get(uri_id_map[row['n.uri']])

    if row['x.uri'] not in uri_id_map:
        name = row['x.uri'].split('#')[1]
        m = p2.Node(name, uri=row['x.uri'])
        m.add_label("owl_Class")
        tx.merge(m, name, 'uri')

        on_property(tx, n, row['p.n4sch__name'], m, rtype=2)
        tx.commit()
        uri_id_map[row['x.uri']] = get_node_id(name=name)
    else:
        m = matcher.get(uri_id_map[row['x.uri']])
