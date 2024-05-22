# SPARQL Extensions

This folder contains functions that extend the functionality of SPARQL.

## JSFunctions
This folder contains JavaScript functions used to extend the functionality of SPARQL with some calculations pertaining to the S2 Geometry. The functions are:
- *checkIfChild.js* which takes two S2 cell binary IDs as input and determines if the former is a descendant of the latter
- *getParentCell.js* which takes as input one S2 cell binary ID and a level of the S2 Geometry, and returns the ancestor of the given cell at that S2 level.
- *getS2BinaryID.js* which takes as input a latitude, longitude and S2 level, and returns the S2 cell that contains the given point at the specified level.

## Rules
This folder contains rules that were used to perform reasoning over the graph. The rules are:
- *geoCoversInfersS2Cell.pie* If the two triples `?x geo:ehCovers ?y` and `?x rdf:type mfd:rasterCell` exist, infer
`?y rdf:type kwg-ont:S2Cell`.

## Queries
**TO-DO**: This folder contains example SPARQL queries that demonstrate how to use the S2 JavaScript functions.
- <i>queryGraph.py</i>: Shows how to set up a connection to, and query, a GraphDB instance using Python.
- 

