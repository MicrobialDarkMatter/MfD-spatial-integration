
### NAMESPACES ###
from rdflib import Namespace
from rdflib.namespace import RDF, XSD, RDFS

RDF = RDF
XSD = XSD
RDFS = RDFS

MFD = Namespace("http://purl.archive.org/domain/mfd#")  # TODO: Create IRI
OBOE = Namespace("http://ecoinformatics.org/oboe/oboe.1.2/oboe-core.owl#")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")
KWG_ONT = Namespace("http://stko-kwg.geog.ucsb.edu/lod/ontology#")

WGS84_POS = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
TIME = Namespace("http://www.w3.org/2006/time#")  # TODO: Check if this is the correct namespace
SCHEMA = Namespace("http://schema.org/")
PROV = Namespace("http://www.w3.org/ns/prov#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
EMPO = Namespace("http://earthmicrobiome.org/protocols-and-standards/empo/")  # TODO: Verify
