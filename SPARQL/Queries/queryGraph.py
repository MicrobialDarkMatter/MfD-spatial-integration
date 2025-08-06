import sys
sys.path.append("/projects/mdm/MfD-spatial-integration/")

import pandas as pd
from sshtunnel import SSHTunnelForwarder
from SPARQLWrapper import SPARQLWrapper, JSON

# From the file in which you write queries, use queryGraph.SERVER.start() to start the tunnel
SERVER = SSHTunnelForwarder(ssh_address_or_host='12.34.5.678',
                            ssh_username='username',
                            ssh_pkey='path/to/key.pem',
                            remote_bind_address=('127.0.0.1', 7200),  # The GraphDB port on the server
                            local_bind_address=('127.0.0.1', 7200))   # Local port to bind to (can be the same as remote if not used locally)
# Make sure to call queryGraph.SERVER.stop() when done

# Load in prefixes for namespaces from the variables file
namespaces = dict()
PREFIXES = ""

with open('/Users/qy90vk/Documents/GitHub/MfD-spatial-integration/toRDF/variables.py') as file:
    exec(file.read(), namespaces)

for key, value in namespaces.items():
    if key in ("__builtins__", "Namespace"):
        continue
    PREFIXES += f"PREFIX {str(key).lower()}: <{value}>\n"


def send_query(query: str):
    """_summary_

    Args:
        query (str): _description_

    Returns:
        _type_: _description_
    """
    endpoint_url = "http://localhost:7200/repositories/mdm"

    sparql = SPARQLWrapper(endpoint_url)

    query = PREFIXES + "\n" + query

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
    SELECT ?s ?p ?o
    WHERE {
        ?s ?p ?o .
    }
    LIMIT 10
    """

    result = send_query(query)
    print(result)

    SERVER.stop()
