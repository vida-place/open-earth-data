# Google-Microsoft Open Buildings - DuckDB tutorial

This tutorial is built around the recently released [Google-Microsoft Open Buildings - combined by VIDA](https://beta.source.coop/repositories/vida/google-microsoft-open-buildings/description) dataset on [Source Cooperative](https://beta.source.coop/). We will show you how to access the cloud-native data formats directly from Source Cooperative using [DuckDB](https://duckdb.org/). We will talk about the partition strategy, its implementations for performance and we will do some tests comparing the merged dataset with just the Google V3 building footprints, which is also hosted on [Source Cooperative](https://beta.source.coop/repositories/cholmes/google-open-buildings/description)

## Setting up DuckDB

Kickstarting this tutorial entails setting up DuckDB v0.9.2. Given our use of Python, a simple pip installation command will suffice:

```python
pip install duckdb
```

Post installation, within our Python script, we'll proceed as follows:

```python
import duckdb

duckdb.sql('INSTALL httpfs')
duckdb.sql('LOAD httpfs')
duckdb.sql('INSTALL spatial')
duckdb.sql('LOAD spatial')
```

With these steps completed, our setup is primed and ready!
The `httpfs` extension empowers us with the capability to read files directly from S3, while the `spatial` extension, as the name suggests, will be leveraged for executing geospatial queries later on in this tutorial. Through these commands, we ensure that our DuckDB setup is well-equipped with the necessary extensions for the tasks ahead.

## Loading GeoParquet directly from S3 to DuckDB

Now that DuckDB is set up and ready, the next step is to load the building footprints data. The dataset is partitioned using two different strategies:

- by country
- by country, sub-partitioned by S2 grid

The sub-partitioning is used to optimise performance when reading GeoParquet files. By setting a cap of 20 millions building footprints per file, we keep the row group size somewhat optimised. Loading a large GeoParquet file directly is a lot slower.  

Let's first load a full (large) country using a single file:

> :warning: **This is a large dataset, loading it using a single file can take a long time on your local machine**

### Load a full (large) country
```python
prefix = "s3://us-west-2.opendata.source.coop/vida/google-microsoft-open-buildings/geoparquet"
partitions = "by_country"
country_iso = "IDN"

# Use single file
duckdb.sql(f"SELECT * FROM '{prefix}/{partitions}/country_iso={country_iso}/{country_iso}.parquet'").show()
```

A more optimised approach is to use DuckDB's `parquet_scan` function:
### Load using the S2 partitions
```python
duckdb.sql(f"SELECT * FROM parquet_scan('{prefix}/by_country_s2/country_iso={country_iso}/*.parquet')").show()
```

The second loading approach is performed under 30 seconds, which represents a big improvement compared to the first one!


## Count the building footprints and check the source

Now, let's delve into some analyses on our dataset, focusing on the country Lesotho due to its smaller size. Initially, we'll fetch the data from S3 and organize it into a DuckDB table:

```python
partitions = "by_country_s2"
country_iso = "LSO"

duckdb.sql(f"CREATE TABLE lso_buildings AS SELECT * FROM parquet_scan('{prefix}/{partitions}/country_iso={country_iso}/*.parquet')")
```

In the code snippet above, we:

- Define the partition scheme and country ISO code as variables.
- Utilize DuckDB's parquet_scan function to read Parquet files directly from S3, conforming to the specified partitioning schema and country filter.
- Employ a CREATE TABLE AS SELECT (CTAS) statement to instantiate a new table lso_buildings, populated with the data retrieved from S3.

Proceeding to the analysis, we'll aggregate the building footprints in our dataset, segmented by their data source:

```python
duckdb.sql(f"SELECT bf_source, COUNT(*) AS buildings_count FROM lso_buildings GROUP BY bf_source;").show()

┌───────────┬─────────────────┐
│ bf_source │ buildings_count │
│  varchar  │     int64       │
├───────────┼─────────────────┤
│ google    │        1394189  │
│ microsoft │         151722  │
└───────────┴─────────────────┘
```

From the output:

- The buildings are grouped by their bf_source column, showing the number of buildings from Google and Microsoft.
- An additional 151,722 buildings from Microsoft are noted, bringing the total count to over 1.5 million for Lesotho.
- The data from Microsoft makes up nearly 10% of the total building footprints in the country.

This analysis sheds light on the substantial contribution from combining these two different datasets.


## S2 partition statistics

Let's delve into analyzing S2 partition statistics. For this exercise, we'll select a country that's sub-divided into multiple S2 grids and compute the buildings count per grid ID. We'll use Australia (AUS) as our subject country due to its extensive geographical spread and substantial urban footprints.

```python
# Using Australia as our sample country (AUS)
partitions = "by_country_s2"
country_iso = "AUS"

# Create a table for storing our queried data
table_query = f"""
    CREATE TABLE aus_buildings AS
    SELECT s2_id, COUNT(geometry) AS buildings_count
    FROM parquet_scan('{prefix}/{partitions}/country_iso={country_iso}/*.parquet')
    GROUP BY(s2_id)
"""
duckdb.sql(table_query)

┌──────────────────────┬─────────────────┐
│        s2_id         │ buildings_count │
│        int64         │     int64       │
├──────────────────────┼─────────────────┤
│  6052837899185946624 │         422107  │
│  3170534137668829184 │        1440431  │
│  7782220156096217088 │        9452987  │
└──────────────────────┴─────────────────┘
```

In the result above, Australia is partitioned into three distinct S2 grids, each with a varying number of buildings. This showcases the geographical distribution and density of buildings across different regions of the country.

Now, let's compute the average number of buildings per S2 grid ID to get a sense of the building density:

```python
query = f"""
    SELECT ROUND(AVG(buildings_count), 0) AS avg_num_buildings
    FROM aus_buildings
"""

duckdb.sql(query).show()

┌───────────────────┐
│ avg_num_buildings │
│      double       │
├───────────────────┤
│         3771842.0 │
└───────────────────┘

```

From the output, it's observed that, on average, there are around 3.7 million buildings per grid ID. This metric provides a rough estimate of building density across different geographical segments within Australia, and can serve as a baseline for further spatial analysis or comparisons with other countries.

## Clip and compare Google V3 with merged dataset

Next up, some spatial analyses! In this section, we'll compare the original Google V3 dataset with our merged dataset to verify the absence of overlap. We'll utilize the Open Buildings dataset available on Source Cooperative for this comparison.

Remember we already loaded Lesotho as a DuckDB table called `lso_buildings` in the previous steps? Lets do the same for the original Google V3 building dataset:

```python
prefix = "s3://us-west-2.opendata.source.coop/google-research-open-buildings/geoparquet-by-country"
country_iso = "LS"
duckdb.sql(f"CREATE TABLE lso_buildings_google AS SELECT * FROM '{prefix}/country_iso={country_iso}/{country_iso}.parquet'")
```

First, let's compare both tables to check for discrepancies in count value. We will first start by counting the original dataset
```python
duckdb.sql("SELECT COUNT(*) FROM lso_buildings_google").show()
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│      1394225 │
└──────────────┘
```

And we can do the same for the merged dataset, selecting only building footprints with Google as the source


```python
duckdb.sql("SELECT COUNT(*) FROM lso_buildings WHERE bf_source = 'google'").show()
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│      1394189 │
└──────────────┘
```

The count shows a difference of 36 buildings between the datasets. This difference could be due to slight variations in processing steps. Both datasets used CGAZ boundaries for partitioning, but our method ran in BigQuery, utilizing the centroids of building footprints to intersect with CGAZ boundaries. This can explain the small discrepancy for geometries that cross the boundary polygon.

For a more precise comparison, executing a spatial join or intersection analysis to find common geometries between the datasets would give more insights. This deeper dive would help understand the spatial relationship and alignment between the original and merged datasets, ensuring data integrity and consistency. So let's do this!

## Compare Google V3 and merged dataset using common geoboundary (AOI)

In this analysis, we aim to ensure that the merging strategy didn't result in duplicate buildings. We'll create a subset of the dataset within a specific geoboundary (Area Of Interest - AOI) and compare the Google V3 dataset with our merged dataset.

Let's start by loading our AOI into a DuckDB table. Given a current limitation with DuckDB 0.9.2 - see issue [#1](https://github.com/vida-impact/open-earth-data/issues/1) - please make sure to download the [boundary file](https://github.com/vida-impact/open-earth-data/raw/main/tutorials/boundary.geojson) first and load it from a local path:

```python
aoi = "/path/to/boundary.geojson"
duckdb.sql(f"CREATE TABLE aoi AS SELECT * FROM ST_Read('{aoi}')")
duckdb.sql("SELECT * FROM aoi").show()
```

Now, let's clip the datasets using our AOI to make the analysis more manageable:

```python
# Clipping the merged dataset
query = """
CREATE TABLE lso_buildings_clipped AS
SELECT ST_Intersection(ST_GeomFromWKB(b.geometry), a.geom) AS geom, b.bf_source
FROM lso_buildings b, aoi a
WHERE ST_Intersects(ST_GeomFromWKB(b.geometry), a.geom);
"""
duckdb.sql(query)

# Clipping the original Google V3 dataset
query = """
CREATE TABLE lso_buildings_google_clipped AS
SELECT ST_Intersection(ST_GeomFromWKB(b.geometry), a.geom) AS geom
FROM lso_buildings_google b, aoi a
WHERE ST_Intersects(ST_GeomFromWKB(b.geometry), a.geom);
"""
duckdb.sql(query)
```

Let's compare the count of buildings in the clipped datasets:

```python
duckdb.sql("SELECT COUNT(*) FROM lso_buildings_google_clipped").show()
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│        13213 │
└──────────────┘
``` 

```python
duckdb.sql("SELECT COUNT(*) FROM lso_buildings_clipped WHERE bf_source = 'google'").show()
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│        13213 │
└──────────────┘
```

The count shows an equal number of buildings, which is a positive sign. Now, let’s ensure there's no overlap or double-counting of building footprints. We'll check for buildings in `lso_buildings_clipped` that don’t intersect with buildings in `lso_buildings_google_clipped`:

```python
query = """
CREATE TABLE non_intersecting AS
SELECT m.*
FROM lso_buildings_clipped m
WHERE NOT EXISTS (
    SELECT 1
    FROM lso_buildings_google_clipped g
    WHERE ST_Intersects(m.geom, g.geom)
);
"""
duckdb.sql(query)
duckdb.sql("SELECT count(*) FROM non_intersecting").show()

┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│          348 │
└──────────────┘
``` 

We find 348 non-intersecting building footprints. Ideally, these should all be sourced from Microsoft. Let’s validate this:

```python
duckdb.sql("SELECT count(*) FROM non_intersecting WHERE bf_source = 'microsoft'").show()
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│          348 │
└──────────────┘
```

The count confirms all 348 non-intersecting buildings are sourced from Microsoft, aligning with our expectations and indicating a successful merge without duplicate entries from Google. These spatial comparisons ensure the integrity of our merged dataset within the specified AOI.

## Exporting to other data formats
Having completed our analyses, it's time to export the data to desired geospatial data formats. In this instance, we'll utilize FlatGeobuf due to its efficiency in handling geospatial data:

```python
output_file = "path/to/output/subset.fgb"
duckdb.sql(f"COPY (SELECT * from lso_buildings_clipped) TO '{output_file}' WITH  (FORMAT GDAL, DRIVER 'FlatGeobuf');")
```

This command exports the clipped dataset to a FlatGeobuf file. Now, if we intend to export the entire country's data:

```python
output_file = "path/to/output/country.fgb"
duckdb.sql(f"COPY (SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) AS geometry from lso_buildings) TO '{output_file}' WITH  (FORMAT GDAL, DRIVER 'FlatGeobuf');")
```

Here, notice the `EXCLUDE` keyword? It's a powerful feature provided by DuckDB, enabling us to exclude a column, apply transformations to it, and include it back in the selection. Since the original geometry is stored in binary format, we transform it using ST_GeomFromWKB before exporting it as a FlatGeobuf file. This ensures the geometry data is correctly formatted for the FlatGeobuf format, facilitating seamless exports of our geospatial data.

With these commands, we can easily export our data to FlatGeobuf format, which can then be used in other geospatial tools or analyses.

## Conclusion
Throughout this tutorial, we navigated the process of setting up DuckDB, importing geospatial data, and performing various analyses on building footprints leveraging the functionalities of the `spatial` and `httpfs` extensions. Through a series of structured steps, we managed to load, process, and analyze building footprints data from different sources, ensuring data integrity and consistency.

We dove into a detailed comparison between the original Google V3 dataset and our merged dataset within a specified geoboundary to ascertain the absence of overlaps or duplicate entries. The analyses were bolstered by the ability to create subsets of the dataset, intersect with predefined geoboundaries, and examine the spatial relationship between datasets.

Moreover, we explored the robust exporting capabilities of DuckDB, which facilitated the conversion of our analyzed data into the efficient FlatGeobuf format, ready for further use or sharing.

In this tutorial, you've picked up some useful tools and techniques that can be applied straightaway to our [Google-Microsoft Open Buildings - combined by VIDA](https://beta.source.coop/vida/google-microsoft-open-buildings/) dataset hosted on Source Cooperative. DuckDB stands out by allowing direct data access from Source Cooperative, which streamlines the exploration and analysis of our dataset. This feature, along with DuckDB's interactive exploration capabilities, simplifies data analysis significantly. Now, with DuckDB, you can dive right into the data on Source Cooperative, utilizing the skills you've acquired to conduct your analysis efficiently and effectively.

## Authors
This tutorial is a collaborative creation of the [VIDA](https://vida.place/) Data Engineering team, brought to you by [Chima](https://github.com/ChimaPaulo), [Shammah](https://github.com/theTrueBoolean), [Iffanice](https://github.com/ZeLynxy), and [Darell](https://github.com/dvd3v).
