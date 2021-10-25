from py4j.java_gateway import launch_gateway
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import csv
import pandas as pd
"""
names = ["s_skos__prefLabel","s_n4sch__comment", "s_rdfs__comment","s_n4sch__label","s_n4sch__name","s_rdfs__label","s_uri",
         "relation_type",
         "o_skos__prefLabel","o_n4sch__comment", "o_rdfs__comment","o_n4sch__label","o_n4sch__name","o_rdfs__label","o_uri"]
dfschema = pd.DataFrame(columns=names)
dfschema = dfschema.fillna("")
dfrels = pd.DataFrame
"""


#gq.run("match (n)-[r1:rdfs__subClassOf]->(b)-[r2:owl__someValuesFrom]->(x),(b)-[r3:owl__onProperty]->(p) return n.n4sch__name, n.n4sch__label, n.uri, type(r2), p.n4sch__name, x.n4sch__name, x.skos__prefLabel, x.uri")

class n4jSimplifier:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.names = ["n.n4sch__name", "n.n4sch__label", "n.uri", "type(r2)", "p.n4sch__name", "x.n4sch__name",
                 "x.skos__prefLabel", "x.uri"]
        self.property_of_df = pd.DataFrame(columns=self.names)
        self.names = ["n.n4sch__name", "n.skos__prefLabel", "n.uri", "type(r)", "m.n4sch__name", "m.skos__prefLabel", "m.uri"]
        self.subclass_of_df = pd.DataFrame(columns=self.names)
        self.names = ["n.n4sch__name", "n.n4sch__label", "n.uri", "type(r2)", "p.n4sch__name", "x.uri"]
        self.has_value_df = pd.DataFrame(columns=self.names)

    def close(self):
        self.driver.close()

    def test(self):
        with self.driver.session() as session:
            print(session.write_transaction())

    def get_properties(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self.write_props_to_df
            )

            for record in result:
                print(record["n"])

        return result

    def write_props_to_df(self):
        query = "match (n)-[r1:rdfs__subClassOf]->(b)-[r2:owl__someValuesFrom]->(x),(b)-[r3:owl__onProperty]->(p) return n.n4sch__name, n.n4sch__label, n.uri, type(r2), p.n4sch__name, x.n4sch__name, x.skos__prefLabel, x.uri order by n.n4sch__name"
        session = None
        response = None

        try:
            session = self.driver.session()
            response = list(session.run(query))
            for record in response:
                self.property_of_df = self.property_of_df.append({
                    'n.n4sch__name': record['n.n4sch__name'],
                    'n.n4sch__label': record['n.n4sch__label'],
                    'n.uri': record['n.uri'],
                    'relation_type': record['type(r2)'],
                    'p.n4sch__name': record['p.n4sch__name'],
                    'x.n4sch__name': record['x.n4sch__name'],
                    'x.skos__prefLabel': record['x.skos__prefLabel'],
                    'x.uri': record['x.uri']
                }, ignore_index=True)
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    def write_subclass_of_df(self):
        query = "MATCH (n)-[r:n4sch__SCO]->(m), (n)-[:rdfs__subClassOf]->(m) return n.n4sch__name, n.skos__prefLabel, n.uri,type(r), m.n4sch__name, m.skos__prefLabel, m.uri"
        session = None
        response = None

        try:
            session = self.driver.session()
            response = list(session.run(query))
            for record in response:
                self.subclass_of_df = self.subclass_of_df.append({
                    'n.n4sch__name': record['n.n4sch__name'],
                    'n.skos__prefLabel': record['n.skos__prefLabel'],
                    'n.uri': record['n.uri'],
                    'relation_type': record['type(r)'],
                    'm.n4sch__name': record['m.n4sch__name'],
                    'm.skos__prefLabel': record['m.skos__prefLabel'],
                    'm.uri': record['m.uri']
                }, ignore_index=True)
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

    def write_has_value_df(self):
        query = "match (n)-[r1:rdfs__subClassOf]->(b)-[r2:owl__hasValue]->(x),(b)-[r3:owl__onProperty]->(p) return n.n4sch__name, n.n4sch__label, n.uri, type(r2), p.n4sch__name, x.uri"
        session = None
        response = None

        try:
            session = self.driver.session()
            response = list(session.run(query))
            for record in response:
                self.has_value_df = self.has_value_df.append({
                    'n.n4sch__name': record['n.n4sch__name'],
                    'n.n4sch__label': record['n.n4sch__label'],
                    'n.uri': record['n.uri'],
                    'relation_type': record['type(r2)'],
                    'p.n4sch__name': record['p.n4sch__name'],
                    'x.uri': record['x.uri']
                }, ignore_index=True)
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise


    def show_df(self):
        print(self.property_of_df)
        print(self.subclass_of_df)
        print(self.has_value_df)
        pass


if __name__ == "__main__":
    scheme = "bolt"
    host_name = "localhost"
    port = 7687
    url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
    user = "neo4j"
    password = "aurpon"
    app = n4jSimplifier(url, user, password)
    app.write_props_to_df()
    app.write_subclass_of_df()
    app.write_has_value_df()
    pd.to_(app.subclass_of_df, "./SCO_df.pkl")
    pd.to_pickle(app.has_value_df, "./HV_df.pkl")
    pd.to_pickle(app.property_of_df, "./PO_df.pkl")
    app.show_df()



















"""
f = open('pizza-ontology-xml.csv', 'r')
reader = csv.reader(f)

headers = next(reader, None)

print(headers)

columns = {}

for h in headers:
    columns[h] = []

for row in reader:
    for h,v in zip(headers,row):
        columns[h].append(v)

print(len(columns['r']))

for i in range(len(columns['r'])):
    sub = columns['ï»¿subject'][i]
    obj = columns['object'][i]

    if sub.startswith('{\"uri\":bnode') or obj.startswith('{\"uri\":bnode'):

        pass
    else:
        sub = sub.strip('{}')
        subcols = sub.split(',"')
        for k in subcols:
            data = k.split(":")
            col, value = data[0].strip('\"'), data[1]
            col = "s_"+col
            dfschema.loc[i, col] = value
        del sub, subcols
        obj = obj.strip('{}')
        objcols = obj.split(',"')

        for k in objcols:
            data = k.split(":")
            col, value = data[0].strip('\"'), data[1]
            col = "o_"+col
            dfschema.loc[i, col] = value
        pass
"""



