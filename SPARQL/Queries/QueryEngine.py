import sys
sys.path.append("/projects/mdm/MfD-spatial-integration/")

import pandas as pd
from sshtunnel import SSHTunnelForwarder
from SPARQLWrapper import SPARQLWrapper, JSON

class QueryEngine:
    """
    QueryEngine class is used to send SPARQL queries to the remote SPARQL endpoint.
    """
    def __init__(self, ssh_address_or_host: str, ssh_username: str, ssh_pkey: str, remote_bind_address: tuple[str, int], local_bind_address: tuple[str, int], endpoint_url: str) -> None:
        """
        Args:
            ssh_address_or_host (str): A string containing the IP address of the server.
            ssh_username (str): A string containing the username to use for the SSH connection.
            ssh_pkey (str): A string containing the path to the private key to use for the SSH connection.
            remote_bind_address (tuple[str, int]): A tuple containing the IP address and port of the GraphDB instance on the remote server
            local_bind_address (tuple[str, int]): A tuple containing the IP address and port of the local server to bind to (can be the same as remote if not used locally)

            endpoint_url (str): A string containing the URL of the remote SPARQL endpoint.
        """
        
        self.__generate_prefixes()
        self.SERVER = SSHTunnelForwarder(ssh_address_or_host=ssh_address_or_host,
                                         ssh_username=ssh_username,
                                         ssh_pkey=ssh_pkey,
                                         remote_bind_address=remote_bind_address,
                                         local_bind_address=local_bind_address)
        self.endpoint_url = endpoint_url
    
    def __generate_prefixes(self) -> None:
        """
        Generates the prefixes to be used in SPARQL queries from the variables.py file.
        """
        
        namespaces = dict()
        self.PREFIXES = ""

        with open(file='/Users/qy90vk/Documents/GitHub/MfD-spatial-integration/toRDF/variables.py', mode='r') as file:
            exec(file.read(), namespaces)

        for key, value in namespaces.items():
            if key in ("__builtins__", "Namespace"):
                continue
            self.PREFIXES += f"PREFIX {str(key).lower()}: <{value}>\n"

    def send_query(self, query: str) -> pd.DataFrame:
        """Prepends the prefixes to the query and sends it to the remote SPARQL endpoint, and returns the results in a pandas DataFrame.

        Args:
            query (str): A python string containing the SPARQL query.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the results of the query.
        """

        self.SERVER.start()
        
        sparql = SPARQLWrapper(self.endpoint_url)
        
        query = self.PREFIXES + query

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()  # JSON object

        self.SERVER.stop()

        variables = results["head"]["vars"]
        data = results["results"]["bindings"]

        df = pd.DataFrame({
            var: [result.get(var, {'value': None})['value'] for result in data]
            for var in variables
        })

        return df
