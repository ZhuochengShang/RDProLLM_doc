## Raptor join operation
Raptor stands for <u>Ra</u>ster-<u>P</u>lus-Vec<u>tor</u>. Raptor operations enable the concurrent processing
of raster and vector data. The following example computes the total area covered by trees for each country in the world.
According to the [land cover legend](https://forobs.jrc.ec.europa.eu/products/glc2000/legend.php),
land cover 1-10 all correspond to trees and we will treat them equally for the sake of this example.
For simplicity, we will use the number of pixels as an approximate for the total area.
Keep in mind that the area of each pixel could be different depending on the CRS in use but we will leave
this calculation out of this example for simplicity.

```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val trees = raster.filterPixels(lc => lc >=1 && lc <= 10)
val countries = sc.shapefile("ne_10m_admin_0_countries.zip")
val result = RaptorJoin.raptorJoinFeature(trees, countries, Seq())
  .map(x => x.feature.getAs[String]("NAME")).countByValue().toMap
println(result)
```
Note: The country dataset can be [downloaded from Natural Earth](https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries.zip)

Check the [Raptor join page](raptor-join.md) for more details on how to use this operation.