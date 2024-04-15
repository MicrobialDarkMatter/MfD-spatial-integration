# Integrating Multi-Modal Spatial Data using Knowledge Graphs
This is the GitHub repository associated with the paper "Integrating Multi-Modal Spatial Data using Knowledge Graphs - a Case Study of Microflora Danica". In here, we present the abstract of the paper, and provide details on how the spatial and microbial aspects of the case study data sets were integrated.

## Abstract
Integrating semantically related, multi-modal, heterogeneous data sources is challenging, especially if one of the modalities includes spatial data, such as field measurements organized in geographical grids.  Since geographical grids can have different rotations, be translated along one or more axes, or have different resolutions, a particular challenge when integrating such data is to reduce the information loss from projecting different grids into a common format. In this paper, we study this problem and sketch a method for integrating such spatial data using knowledge graphs.  We discuss this solution in the context of a real-world use case, where we integrate geographically annotated microbial data (Microflora Danica) as well as environmental data to enable joint analysis. The first results of our experiments show that our method reduces the information loss compared to baseline methods.

## The Microflora Danica Dataset
> Nymann, T., Sørensen, E. A., Sereika, M., Jørgensen, V. R., Mølvang Dall, S., Knutsson, S., Petriglieri, F., Dottorini, G., Singleton, C. M., Karst, S. M., Dueholm, M. K. D., Nielsen, P. H., & Albertsen, M. (2022). Microflora Danica - The Microbiome of Denmark. P52. Poster presented at The Danish Microbiological Society Congress (DMS2022), Copenhagen, Denmark.

Estimates for the size of the Microflora Danica dataset are available in the table below. Due to the size of files, the reads/sequence is based on a single file.
|               |               |                    |             | Read Length             | ------  | ------- |  ---- |
|---------------|--------------:|-------------------:|------------:|------------------------:|--------:|--------:|------:|
| **Dataset**   | **Size**      | **Reads/Sequence** |**Sequences**| _Average_               |  _Stdv_ |   _Max_ | _Min_ |
| Long Reads    |         13 TB |         24,000,000 |          154|             4,500 bases |   4,000 |  70,000 |   200 |
| Short Reads   |         15 TB |         12,000,000 |       25,878|               149 bases |       8 |     151 |   100 |

The figure below illustrates the process for the creation of the Microflora Danica dataset.
![](/readmeFigures/MfDDataCollectionOverview.drawio.svg)

## Environmental Raster Files
The goal is to integrate the knowledge regarding microbes from the Microflora Danica Database with knowledge regarding the environment of the sites where the microbes were sampled. For this, we use the [EcoDes-DK15 Dataset](#ecodes-dk15) and [Soil Maps](#soil-maps).

### EcoDes-DK15
> Assmann, J. J., Moeslund, J. E., Treier, U. A., and Normand, S.: "EcoDes-DK15: high-resolution ecological descriptors of vegetation and terrain derived from Denmark's national airborne laser scanning data set", Earth Syst. Sci. Data, 14, 823–844, <https://doi.org/10.5194/essd-14-823-2022>, 2022.

Data available from [Zenodo](https://zenodo.org/doi/10.5281/zenodo.4756556>).

![Examples of features measured in the EcoDes-DK15 dataset](/readmeFigures/EcoDes-DK15.png)

### Soil Maps
> Gomes, L. C., Beucher, A. M., Møller, A. B., Iversen, B. V., Børgesen, C. D., Adetsu, D. V., Sechu, G. L., Heckrath, G. J., Koch, J., Adhikari, K., Knadel, M., Lamandé, M., Greve, M. B., Jensen, N. H., Gutierrez, S., Balstrøm, T., Koganti, T., Roell, Y., Peng, Y., Greve, M. H. (2023) "Soil assessment in Denmark: Towards soil functional mapping and beyond". _Front. Soil Sci._ 3:1090145. doi: [10.3389/fsoil.2023.1090145](https://doi.org/10.3389/fsoil.2023.1090145)

![Examples of features measured in the Soil Maps dataset](/readmeFigures/soilMaps.png)


## Data Integration Approach
### Spatial Integration Layer - S2 Geometry
In order to handle the integration of raster files with potentially different translations, rotations, or resolutions, we integrate them into a common grid, the [S2 Geometry](http://s2geometry.io), as all raster cells in a raster file can be mapped to a set of corresponding cells in the S2 Geometry, regardless of the transformations on the raster.

The S2 Geometry is a 31-level hierarchical grid that decomposes Earth into a hierarchy of cells. At the top-most level (level 0) of the hierarchy, Earth is divided into six cells perfectly covering it, while each higher level of granularity subdivides each cell into four children, such that there are $24$ cells at level 1, $96$ cells at level 2, and so on.

### Spatial Data
Ontologies for the spatial part of the KG design:

| **Prefix** | **IRI**                                                  |
|------------|----------------------------------------------------------|
| _geo_      | <http://www.opengis.net/ont/geosparql#>                  |
| _oboe_     | <http://ecoinformatics.org/oboe/oboe.1.2/oboe-core.owl#> |
| _kwg-ont_  | <http://stko-kwg.geog.ucsb.edu/lod/ontology#>            |
| _rdf_      | <http://www.w3.org/1999/02/22-rdf-syntax-ns#>            |

We design the KG for the spatial aspect of the data integration as shown in the following figure.
![KG Design for Spatial Part](/readmeFigures/kgDesignSpatial.png)

### Microbial Data

We design the KG for the microbial aspect of the data integration as shown in the following figure.
![KG Design for Spatial Part](/readmeFigures/kgDesignMicrobial.png)

# Running the Code
The source code for mapping raster files to the S2 Geometry is described below.

## /S2Integration/
The mapping folder stores the Python code for creating the mappings. 
The code for the raster integration runs in three steps:
1. Retrieving the raster cells.
2. Mapping unique raster cells to the S2 Geometry.
3. Converting the mappings to RDF.

**Retrieving the raster pixels.**
The file _[retrieve_raster_cells.py](S2Integration%2Fretrieve_raster_cells.py)_ extracts all raster cells with a corresponding value for each raster file. 
The corners in easting and northing are stored in a parquet file along with the measured value. It is stored in a file to not clock up the memory.

**Mapping unique raster cells to the S2 Geometry.**
The mappings to the S2 Geometry are handled in _[parallel_map_raster_cells_to_s2.py](S2Integration%2Fparallel_map_raster_cells_to_s2.py)_. 
The unique raster cells are obtained from the parquet files, which are then split in chunks to afford the raster cells to be mapped in parallel. 
The mappings are saved in a parquet file.

**Converting the mappings to RDF.**
In _[spatial_to_rdf.py](S2Integration%2Fspatial_to_rdf.py)_, the mappings are converted to RDF using the saved parquet files. 
This is specific to our case study, and as so, likely not fully applicable to other mappings.

Running [main.py](S2Integration%2Fmain.py), the point-based samples are mapped to the S2 Geometry as well as integrating the raster files to the S2 Geometry.

We have prioritized the integration of raster files, however, the code for the point-based samples can be found in [map_points_to_s2.py](S2Integration%2Fmap_points_to_s2.py).

## /toRDF/
Running the [spatial_to_rdf.py](toRDF%2Fspatial_to_rdf.py), the mappings are converted to RDF using the saved parquet files.

## /example/
In the example folder, a small subset of the [EcoDes-DK15 Teaser Dataset](https://zenodo.org/records/6035188) is downloaded to demonstrate the mappings to the S2 Geometry.

Moreover, the outputs from the mappings are saved.