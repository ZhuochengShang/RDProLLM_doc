# Raster processing using RDPro
Raster data is a very popular format that stores geospatial data in multidimensional arrays.
The prime example of geospatial raster data is satellite data that come in the form of image-like data.
Beast provides a special component for processing raster data called RDPro, Raster Distributed Processor.
This page describes the basic RDPro support in Beast.

## Setup
- Follow the [setup page](dev-setup.md) to prepare your project for using Beast.
- If you use Scala, add the following line in your code to import all Beast features.
```scala
import edu.ucr.cs.bdlab.beast._
```