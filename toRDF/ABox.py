import sys
sys.path.append("/projects/mdm/MfD-spatial-integration/")

from rdflib import Graph, URIRef

import gzip

from variables import *


def create_abox(save_file):
    with gzip.open(filename=save_file, mode="at", encoding="utf-8") as triple_file:
        G = Graph()

        G.add(triple=(URIRef(MFD + "Site"),
                      URIRef(RDFS.subClassOf),
                      URIRef(WGS84_POS + "Point")))

        for i in range(1, 4):
            G.add(triple=(URIRef(MFD + f"Habitat{i}"),
                          URIRef(RDFS + "subClassOf"),
                          URIRef(SKOS + "Concept")))

        G.add(triple=(URIRef(MFD + "RasterFile"),
                      URIRef(RDFS.subClassOf),
                      URIRef(OBOE + "ObservationCollection")))

        G.add(triple=(URIRef(MFD + "RasterCell"),
                      URIRef(RDFS.subClassOf),
                      URIRef(OBOE + "Observation")))

        triple_file.write(G.serialize(format='nt'))
