import sys
sys.path.append("/projects/mdm/MfD-spatial-integration/")

from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import RDF, XSD, RDFS
from babelgrid import Babel
import gzip
import re

from variables import *
from spatial_to_rdf import convert_s2_id_to_bit_id


from S2Integration.misc.LogFile import LogFile


RESOLUTION = 24  # TODO: Fix resolution if it changes


def not_nan(value):
    return value == value


def row_graphs_to_graph(dataframe, function):
    """Convert a dataframe to a graph using a function."""
    G = Graph()
    for i in range(dataframe.shape[0]):
        G += function(dataframe.iloc[i])
    return G


def save_graph(save_buffer, graph):
    with gzip.open(filename=save_buffer, mode="at", encoding="utf-8") as triple_file:
        triple_file.write(graph.serialize(format="nt"))


def extract_people_info(input_string):
    # Regular expression to match the pattern
    pattern = r'^([^<]+) <([^>]+)>$'

    # Match the pattern
    match = re.match(pattern, input_string)

    if match:
        # Extracting first name(s), last name, and email
        names, email = match.groups()
        names = names.split()
        first_names = " ".join(names[:-1])
        last_name = names[-1]
        return first_names, last_name, email.replace(" ", "")
    else:
        return None, None, None


def get_s2_cell_hex_from_sample(lat, lon):
    """Get the S2 cell ID from a sample."""
    grid = Babel("S2")
    s2_id = grid.geo_to_tile(lat=lat, lon=lon, resolution=RESOLUTION).tile_id
    return convert_s2_id_to_bit_id(s2_id)


def get_id_mappings(mfdo):
    hierarchy = {}

    for idx, row in mfdo.iterrows():  # Loop through each row in the habitat dataframe
        parent = row["mfd_hab1"]
        child = row["mfd_hab2"]
        root = row["mfd_hab3"]

        if parent != parent:  # If entry is NaN, continue
            continue
        parent = parent.strip()  # Misalignment with spaces
        hierarchy.setdefault(parent, {})
        if child != child:  # If entry is NaN, continue
            continue
        child = child.strip()
        hierarchy[parent].setdefault(child, [])
        if root != root:  # If entry is NaN, continue
            continue
        root = root.strip()
        hierarchy[parent][child].append(root)

    id_mappings = {}
    root_id = 0

    for root, child in hierarchy.items():
        child_id = 0
        root_id += 1
        id_mappings[root] = str(root_id).zfill(4) + "_0000_0000"
        for c, leafs in child.items():
            leaf_id = 0
            child_id += 1
            id_mappings[c] = str(root_id).zfill(4) + "_" + str(child_id).zfill(4) + "_0000"
            for leaf in leafs:
                leaf_id += 1
                id_mappings[leaf] = str(root_id).zfill(4) + "_" + str(child_id).zfill(4) + "_" + str(leaf_id).zfill(4)

    return id_mappings


def seq_row_to_graph(row):
    """Convert a row of the sequencing metadata to a graph."""
    G = Graph()

    # Check NaN in sequence ID
    if not not_nan(row["seq_id"]):
        return G

    # sample to sequence
    G.add(triple=(URIRef(MFD + row["fieldsample_barcode"]),
                  URIRef(MFD + "hasSequence"),
                  URIRef(MFD + row["seq_id"])))
    # type
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(RDF.type),
                  URIRef(MFD + "Sequence")))

    # sequence to extraction
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasExtractionID"),
                  URIRef(MFD + row["extraction_id"])))
    # type
    G.add(triple=(URIRef(MFD + row["extraction_id"]),
                  URIRef(RDF.type),
                  URIRef(MFD + "Extraction")))

    # sequence to extraction plate
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasExtractionPlateID"),
                  URIRef(MFD + row["extractionplate_id"])))
    # type
    G.add(triple=(URIRef(MFD + row["extractionplate_id"]),
                  URIRef(RDF.type),
                  URIRef(MFD + "ExtractionPlate")))

    # sequence to extraction row
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasExtractionRow"),
                  Literal(row["extraction_row"], datatype=XSD.string)))

    # sequence to extraction column
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasExtractionColumn"),
                  Literal(row["extraction_col"], datatype=XSD.integer)))

    # sequence to extraction concentration
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasExtractionConc"),
                  Literal(row["extraction_conc"], datatype=XSD.decimal)))

    # sequence to extraction method
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasExtractionMethod"),
                  URIRef(MFD + row["extraction_method"].strip(" "))))  # Removes trailing space
    # type
    G.add(triple=(URIRef(MFD + row["extraction_method"].strip(" ")),
                  URIRef(RDF.type),
                  URIRef(MFD + "ExtractionMethod")))

    # sequence to library
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasLibraryID"),
                  URIRef(MFD + row["library_id"])))
    # type
    G.add(triple=(URIRef(MFD + row["library_id"]),
                  URIRef(RDF.type),
                  URIRef(MFD + "Library")))

    # sequence to library plate
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasLibraryPlateID"),
                  URIRef(MFD + row["libraryplate_id"])))
    # type
    G.add(triple=(URIRef(MFD + row["libraryplate_id"]),
                  URIRef(RDF.type),
                  URIRef(MFD + "LibraryPlate")))

    # sequence to library row
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasLibraryRow"),
                  Literal(row["library_row"], datatype=XSD.string)))

    # sequence to library conc
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasLibraryConc"),
                  Literal(row["library_conc"], datatype=XSD.decimal)))

    # sequence to library method
    G.add(triple=(URIRef(MFD + row["seq_id"]),
                  URIRef(MFD + "hasLibraryMethod"),
                  URIRef(MFD + row["library_method"])))
    # type
    G.add(triple=(URIRef(MFD + row["library_method"]),
                  URIRef(RDF.type),
                  URIRef(MFD + "LibraryMethod")))

    return G


def field_row_to_graph(row, id_mappings=None):
    G = Graph()

    # sample type
    G.add(triple=(URIRef(MFD + row["fieldsample_barcode"]),
                  URIRef(RDF.type),
                  URIRef(MFD + "Sample")))

    if not_nan(row["project_id"]):
        G.add(triple=(URIRef(MFD + row["fieldsample_barcode"]),
                      URIRef(MFD + "relatedProject"),
                      URIRef(MFD + row["project_id"])))
        # type
        G.add(triple=(URIRef(MFD + row["project_id"]),
                      URIRef(RDF.type),
                      URIRef(SCHEMA + "Project")))

    if not_nan(row["sampling_date"]):
        # sample to sampling_date
        G.add(triple=(URIRef(MFD + row["fieldsample_barcode"]),
                      URIRef(TIME + "inXSDDate"),
                      Literal(row["sampling_date"], datatype=XSD.date)))

    if not_nan(row["coords_reliable"]):
        # sample to reliable coordinates
        G.add(triple=(URIRef(MFD + row["fieldsample_barcode"]),
                      URIRef(MFD + "hasReliableCoordinates"),
                      Literal(row["coords_reliable"], datatype=XSD.string)))

    if not_nan(row["site_bnode"]):
        G.add(triple=(URIRef(MFD + row["fieldsample_barcode"]),
                      URIRef(WGS84_POS + "location"),
                      row["site_bnode"]))
        # type
        G.add(triple=(row["site_bnode"],
                      URIRef(RDF.type),
                      URIRef(MFD + "Site")))

        # Add habitat
        if id_mappings:
            if not_nan(row["mfd_hab3"]):
                G.add(triple=(row["site_bnode"],
                              URIRef(MFD + "hasHabitat"),
                              URIRef(MFD + id_mappings[row["mfd_hab3"].strip()])))
            elif not_nan(row["mfd_hab2"]):
                G.add(triple=(row["site_bnode"],
                              URIRef(MFD + "hasHabitat"),
                              URIRef(MFD + id_mappings[row["mfd_hab2"].strip()])))
            elif not_nan(row["mfd_hab1"]):
                G.add(triple=(row["site_bnode"],
                              URIRef(MFD + "hasHabitat"),
                              URIRef(MFD + id_mappings[row["mfd_hab1"].strip()])))
            else:
                pass  # NO HABITAT INFORMATION

        if not_nan(row["latitude"]):
            # site to latitude
            G.add(triple=(row["site_bnode"],
                          URIRef(WGS84_POS + "latitude"),
                          Literal(row["latitude"], datatype=XSD.decimal)))

        if not_nan(row["longitude"]):
            # site to longitude
            G.add(triple=(row["site_bnode"],
                          URIRef(WGS84_POS + "longitude"),
                          Literal(row["longitude"], datatype=XSD.decimal)))

        # map to S2 cell
        if not_nan(row["latitude"]) and not_nan(row["longitude"]):
            s2_cell = get_s2_cell_hex_from_sample(lat=row["latitude"], lon=row["longitude"])
            # site to S2 cell
            G.add(triple=(row["site_bnode"],
                          URIRef(GEO + "ehCoveredBy"),
                          URIRef(KWG_ONT + s2_cell)))

            G.add(triple=(URIRef(KWG_ONT + s2_cell),
                          URIRef(RDF.type),
                          URIRef(KWG_ONT + "S2Cell")))

        if not_nan(row["sitename"]):
            # site to sitename
            G.add(triple=(row["site_bnode"],
                          URIRef(RDFS + "label"),
                          Literal(row["sitename"], datatype=XSD.string)))

        if not_nan(row["cell.10km"]):
            # site to cell10km
            G.add(triple=(row["site_bnode"],
                          URIRef(MFD + "in10kmCell"),
                          URIRef(MFD + row["cell.10km"])))
            # type
            G.add(triple=(URIRef(MFD + row["cell.10km"]),
                          URIRef(RDF.type),
                          URIRef(MFD + "Cell10km")))

        if not_nan(row["cell.1km"]):
            # site to cell1km
            G.add(triple=(row["site_bnode"],
                          URIRef(MFD + "in1kmCell"),
                          URIRef(MFD + row["cell.1km"])))
            # Type
            G.add(triple=(URIRef(MFD + row["cell.1km"]),
                          URIRef(RDF.type),
                          URIRef(MFD + "Cell1km")))

    return G


def project_row_to_graph(row):
    G = Graph()

    if not_nan(row["description"]):
        # project to description
        G.add(triple=(URIRef(MFD + row["project_id"]),
                      URIRef(SCHEMA + "description"),
                      Literal(row["description"], datatype=XSD.string)))

    if not_nan(row["extended_metadata"]):
        # project to extended metadata
        G.add(triple=(URIRef(MFD + row["project_id"]),
                      URIRef(SCHEMA + "keywords"),
                      Literal(row["extended_metadata"], datatype=XSD.string)))

    if not_nan(row["comment"]):
        # project to comment
        G.add(triple=(URIRef(MFD + row["project_id"]),
                      URIRef(SCHEMA + "comment"),
                      Literal(row["comment"], datatype=XSD.string)))

    if not_nan(row["responsible"]):
        for responsible in row["responsible"].split("; "):
            first_names, last_name, email = extract_people_info(responsible)
            if email:
                person_iri = URIRef(MFD + "person#" + email)
                # project to responsible
                G.add(triple=(URIRef(MFD + row["project_id"]),
                              URIRef(MFD + "responsible"),
                              person_iri))

                # type
                G.add(triple=(person_iri,
                              URIRef(RDF.type),
                              URIRef(SCHEMA + "Person")))

                # responsible to email
                G.add(triple=(person_iri,
                              URIRef(SCHEMA + "email"),
                              URIRef(MFD + email)))

                # type
                G.add(triple=(URIRef(MFD + email),
                                URIRef(RDF.type),
                                URIRef(MFD + "Email")))

                if first_names:
                    # responsible to given name
                    G.add(triple=(person_iri,
                                  URIRef(SCHEMA + "givenName"),
                                  Literal(first_names, datatype=XSD.string)))

                if last_name:
                    # responsible to last name
                    G.add(triple=(person_iri,
                                  URIRef(SCHEMA + "lastName"),
                                  Literal(last_name, datatype=XSD.string)))

    if not_nan(row["people"]):
        for people in row["people"].split("; "):
            first_names, last_name, email = extract_people_info(people)
            if email:
                person_iri = URIRef(MFD + "person#" + email)
                # project to people
                G.add(triple=(URIRef(MFD + row["project_id"]),
                              URIRef(PROV + "wasAttributedTo"),
                              person_iri))
                # type
                G.add(triple=(person_iri,
                              URIRef(RDF.type),
                              URIRef(SCHEMA + "Person")))

                # person to email
                G.add(triple=(person_iri,
                              URIRef(SCHEMA + "email"),
                              URIRef(MFD + email)))

                # type
                G.add(triple=(URIRef(MFD + email),
                              URIRef(RDF.type),
                              URIRef(MFD + "Email")))

                if first_names:
                    # person to given name
                    G.add(triple=(person_iri,
                                  URIRef(SCHEMA + "givenName"),
                                  Literal(first_names, datatype=XSD.string)))

                if last_name:
                    # person to last name
                    G.add(triple=(person_iri,
                                  URIRef(SCHEMA + "lastName"),
                                  Literal(last_name, datatype=XSD.string)))

    return G


def iterative_add_habitat(row, id_mappings, habitat_level):
    G = Graph()

    for i in reversed(range(1, habitat_level + 1)):
        if not_nan(row[f"mfd_hab{i}"]):
            habitat_id = id_mappings[row[f"mfd_hab{i}"].strip()]
            habitat = str(row[f"mfd_hab{i}"].strip())

            G.add(triple=(URIRef(MFD + habitat_id),
                          URIRef(RDFS.label),
                          Literal(habitat, datatype=XSD.string)))

            G.add(triple=(URIRef(MFD + habitat_id),
                          URIRef(RDF.type),
                          URIRef(MFD + f"Habitat{i}")))

            if not_nan(row["Natura2000"]) and habitat_level == i:
                G.add(triple=(URIRef(MFD + habitat_id),
                              URIRef(MFD + "hasNatura2000Concept"),
                              URIRef(MFD + row["Natura2000"])))

                G.add(triple=(URIRef(MFD + row["Natura2000"]),
                              URIRef(RDF.type),
                              URIRef(MFD + "Natura2000Concept")))


            if not_nan(row["EUNIS"]) and habitat_level == i:
                G.add(triple=(URIRef(MFD + habitat_id),
                              URIRef(MFD + "hasEUNISConcept"),
                              URIRef(MFD + row["EUNIS"])))  # TODO: Write MFD + "EUNISConcept/" + row["EUNIS"]? (Same for other)

                # type
                G.add(triple=(URIRef(MFD + row["EUNIS"]),
                                URIRef(RDF.type),
                                URIRef(MFD + "EUNISConcept")))

            if not_nan(row["EMPO"]):
                empo_1_2_3 = row["EMPO"].split(";")
                G.add(triple=(URIRef(MFD + habitat_id),
                              URIRef(MFD + f"hasEMPO{i}Concept"),
                              URIRef(MFD + empo_1_2_3[i - 1].replace(" ", ""))))  # TODO: Check if EMPO should be used

                # type
                G.add(triple=(URIRef(MFD + empo_1_2_3[i - 1].replace(" ", "")),
                              URIRef(RDF.type),
                              URIRef(MFD + f"EMPO{i}Concept")))

            if i > 1:
                G.add(triple=(URIRef(MFD + habitat_id),
                              URIRef(SKOS + "broadMatch"),
                              URIRef(MFD + id_mappings[row[f"mfd_hab{i - 1}"]])))
                #
            else:
                G.add(triple=(URIRef(MFD + habitat_id),
                              URIRef(MFD + "hasAreaType"),
                              URIRef(MFD + row["mfd_areatype"])))

                # type
                G.add(triple=(URIRef(MFD + row["mfd_areatype"]),
                                URIRef(RDF.type),
                                URIRef(MFD + "AreaType")))

                G.add(triple=(URIRef(MFD + habitat_id),
                              URIRef(MFD + "hasSampleType"),
                              URIRef(MFD + row["mfd_sampletype"])))

                # type
                G.add(triple=(URIRef(MFD + row["mfd_sampletype"]),
                                URIRef(RDF.type),
                                URIRef(MFD + "SampleType")))

    return G


def ontology_row_to_graph(row, id_mappings):
    G = Graph()  # Initialize an empty graph

    if not_nan(row["mfd_hab3"]):
        G += iterative_add_habitat(row, id_mappings, 3)

    elif not_nan(row["mfd_hab2"]):
        G += iterative_add_habitat(row, id_mappings, 2)

    elif not_nan(row["mfd_hab1"]):
        G += iterative_add_habitat(row, id_mappings, 1)

    else:
        pass

    return G


def row_otu_to_rdf(row):
    G = Graph()

    # print(row)
    # print(" ROW INDEX ")
    # print(list(row.index))
    # print(" ROW INDEX SLICE ")
    # print((row.index[1:-7]))

    # OTU and samples
    for sample in row.index[1:-7]:
        if row[sample] == 0:  # Don't add OTUs of no abundance
            continue

        measurement_bnode = BNode()
        G.add(triple=(URIRef(MFD + sample),
                      URIRef(OBOE + "hasMeasurement"),
                      measurement_bnode))

        G.add(triple=(measurement_bnode,
                      URIRef(RDF.type),
                      URIRef(OBOE + "Measurement")))

        G.add(triple=(measurement_bnode,
                      URIRef(OBOE + "hasValue"),
                      Literal(row[sample], datatype=XSD.integer)))

        G.add(triple=(measurement_bnode,
                      URIRef(OBOE + "containsMeasurementsOfType"),
                      URIRef(MFD + row["OTU"])))

    # OTU and species
    previous_taxon = None
    for taxon in reversed(row.index[-7:]):
        if row[taxon] == row[taxon]:
            if not previous_taxon:
                G.add(triple=(URIRef(MFD + row["OTU"]),
                              URIRef(MFD + "hasTaxonMapping"),  # TODO: Likely to change
                              URIRef(MFD + row[taxon])))
            else:
                G.add(triple=(URIRef(MFD + previous_taxon),
                              URIRef(RDFS + "subClassOf"),
                              URIRef(MFD + row[taxon])))

            G.add(triple=(URIRef(MFD + row[taxon]),
                          URIRef(RDF.type),
                          URIRef(MFD + taxon)))  # TODO: Map to NCBITaxon instead

            previous_taxon = row[taxon]

    return G


def save_otu_graph(otu, save_buffer):
    log = LogFile(log_file="otu.txt")
    with gzip.open(filename=save_buffer, mode="at", encoding="utf-8") as triple_file:
        for i in range(otu.shape[0]):
            graph = row_otu_to_rdf(otu.iloc[i])

            triple_file.write(graph.serialize(format='nt'))

            log.update_log_idx(log.get_log_idx() + 1)


if __name__ == "__main__":
    # # # # Microflora Danica data # # # #
    import pandas as pd
    from io import BytesIO
    import requests
    import rasterio

    # Sample data
    field_metadata_dir = "https://github.com/cmc-aau/mfd_metadata/raw/main/analysis/releases/latest_mfd_db.xlsx"
    field_metadata_file = requests.get(field_metadata_dir)
    field_metadata = pd.read_excel(BytesIO(field_metadata_file.content))

    sites_df = field_metadata[["latitude", "longitude"]].drop_duplicates()  # adding b_nodes to sample sites
    sites_df["site_bnode"] = [BNode() for i in range(sites_df.shape[0])]
    field_metadata = field_metadata.merge(sites_df, on=["latitude", "longitude"], how="left")
    
    # Sequence data
    seq_metadata_dir = "https://raw.githubusercontent.com/cmc-aau/mfd_metadata/main/data/metadata/general/latest_corrected_combined_metadata.csv"
    seq_metadata = pd.read_csv(seq_metadata_dir)
    # seq_metadata = seq_metadata[seq_metadata["fieldsample_barcode"].isin(field_metadata["fieldsample_barcode"])]
    
    # Project data
    projects_dir = "https://github.com/cmc-aau/mfd_metadata/raw/main/analysis/releases/latest_mfd_projects.xlsx"
    projects_file = requests.get(projects_dir)
    projects = pd.read_excel(BytesIO(projects_file.content))
    # projects = projects[projects["project_id"].isin(field_metadata["project_id"])]

    # Habitat
    from functools import partial

    mfdo_dir = "https://github.com/cmc-aau/mfd_metadata/raw/main/data/ontology/latest_mfd-habitat-ontology.xlsx"
    mfdo_file = requests.get(mfdo_dir)
    mfdo = pd.read_excel(BytesIO(mfdo_file.content),
                         dtype={'mfd_sampletype': str, 'mfd_areatype': str, 'mfd_hab1_code': str, 'mfd_hab1': str,
                                'mfd_hab2_code': str, 'mfd_hab2': str, 'mfd_hab3_code': str, 'mfd_hab3': str,
                                'Natura2000': str, 'EUNIS': str, 'EMPO': str})

    id_mappings = get_id_mappings(mfdo)

    ontology_row_to_graph = partial(ontology_row_to_graph, id_mappings=id_mappings)

    # Update the field metadata with habitat information
    field_metadata.merge(mfdo, on=["mfd_sampletype", "mfd_areatype", "mfd_hab1", "mfd_hab2", "mfd_hab3"], how="left")
    field_row_to_graph = partial(field_row_to_graph, id_mappings=id_mappings)




    # # # # Convert the data to RDF # # # #
    save_graph(save_buffer="fieldsamples.nt.gz",
               graph=row_graphs_to_graph(field_metadata, field_row_to_graph))
    
    save_graph(save_buffer="sequencing.nt.gz",
               graph=row_graphs_to_graph(seq_metadata, seq_row_to_graph))
    
    save_graph(save_buffer="projects.nt.gz",
               graph=row_graphs_to_graph(projects, project_row_to_graph))

    save_graph(save_buffer="habitat.nt.gz",
               graph=row_graphs_to_graph(mfdo, ontology_row_to_graph))

    # # OTU data
    # otu_path = "/projects/microflora_danica/sub_projects/phylotables/analysis/release/2024-03-07_arcbac_MFD_samples_phylotabel_release.csv"
    # otu_df = pd.read_csv(otu_path)
    # save_otu_graph(otu=otu_df, save_buffer="otu.nt.gz")
    # fieldsamples = field_metadata["fieldsample_barcode"].unique()
    # cols_to_drop = [col for col in otu_df.columns if col not in fieldsamples and "MFD" in col]
    # otu_df = otu_df.drop(columns=cols_to_drop)
