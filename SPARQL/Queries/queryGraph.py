import re
import json

import pandas as pd
from sshtunnel import SSHTunnelForwarder
from SPARQLWrapper import SPARQLWrapper, JSON

# From the file in which you write queries, use queryGraph.SERVER.start() to start the tunnel
SERVER = SSHTunnelForwarder(ssh_address_or_host='12.34.5.678',
                            ssh_username='username',
                            ssh_pkey='path/to/key.pem',
                            remote_bind_address=('127.0.0.1', 7200),  # The GraphDB port on the server
                            local_bind_address=('127.0.0.1', 7200))    # Local port to bind to (can be the same as remote if not used locally)
# Make sure to call queryGraph.SERVER.stop() when done

def send_query(query: str):
    endpoint_url = "http://localhost:7200/repositories/mdm"

    sparql = SPARQLWrapper(endpoint_url)

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()  # JSON object

    variables = results["head"]["vars"]
    data = results["results"]["bindings"]

    df = pd.DataFrame({
        var: [result.get(var, {'value': None})['value'] for result in data]
        for var in variables
    })

    return df


if __name__ == "__main__":
    SERVER.start()

    query = """
    PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX mfd: <http://purl.archive.org/domain/mfd#>
    PREFIX oboe: <http://ecoinformatics.org/oboe/oboe.1.2/oboe-core.owl#>
    PREFIX empo: <http://earthmicrobiome.org/protocols-and-standards/empo/>
    PREFIX schema: <http://schema.org/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX time: <http://www.w3.org/2006/time#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT ?s ?p ?o
    WHERE {
        ?s ?p ?o .
        }
    """

    result = send_query(query)
    print(result)

    SERVER.stop()
