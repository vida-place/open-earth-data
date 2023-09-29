# Google-Microsoft Open Buildings - DuckDB tutorial

This tutorial is built around the recently released [Google-Microsoft Open Buildings - combined by VIDA](https://beta.source.coop/repositories/vida/google-microsoft-open-buildings/description) dataset on [Source Cooperative](https://beta.source.coop/). We will show you how to access the cloud-native dataformats directly from Source Cooperative using [DuckDB](https://duckdb.org/). We will talk about the partition strategy, its implementations for performance and we will do some tests comparing the merged dataset with just the Google V3 building footprints, which is also hosted on [Source Cooperative](https://beta.source.coop/repositories/cholmes/google-open-buildings/description)

## Setting up DuckDB

```python
pip install duckdb
```

```python
import duckdb

duckdb.sql('INSTALL httpfs')
duckdb.sql('LOAD httpfs')
duckdb.sql('INSTALL spatial')
duckdb.sql('LOAD spatial')
```

## Loading GeoParquet directly from S3 to DuckDB

- show difference in loading time partition vs non-partition


### Load a full (large) country
```python
prefix = "s3://us-west-2.opendata.source.coop/vida/google-microsoft-open-buildings/geoparquet"
country_iso = "IDN"

# Use single file
duckdb.sql(f"SELECT * FROM '{prefix}/by_country/country_iso={country_iso}/{country_iso}.parquet'").show()
```

### Load using the S2 partitions
```python
duckdb.sql(f"SELECT * FROM parquet_scan('{prefix}/by_country_s2/country_iso={country_iso}/*.parquet')").show()
```

write something about execution time

## Count the building footprints and check the source

For this we are going to use Lesotho, a smaller country. First we load the data from S3 and store it as a DuckDB table:

```python
country_iso = "LSO"
# TODO: store as lso_buildings table
duckdb.sql("")
```

Now we can count the buildings from the stored DuckDB table:

```
# TODO: write query that outputs count of both building footprint sources
```

### Count the buildings which have no associated country
```python
country_iso_null = "None"
duckdb.sql(f"SELECT COUNT(*) FROM parquet_scan('{prefix}/by_country_s2/country_iso={country_iso_null}/*.parquet')").show()
duckdb.sql("")
```

## Generate some S2 partition statistics
### Obtain the average number of buildings in an S2 grid

```python
# Using Australia as our sample country (AUS)
country_iso = 'AUS'

# Create a table for storing our queried data
table_query = f"""
    CREATE TABLE tempTable AS
    SELECT s2_id, COUNT(geometry) AS building_count
    FROM parquet_scan('{prefix}/by_country_s2/country_iso={country_iso}/*.parquet')
    GROUP BY(s2_id)
"""
duckdb.sql(table_query)

# obtain the average buildings per S2
avg_query = f"""
    SELECT AVG(building_count) AS avg_num_buildings
    FROM tempTable
"""

duckdb.sql(avg_query).show()
```

### Obtain the S2 grid with the maximum number of building
```python
max_query = f"""
    SELECT s2_id, MAX(building_count)
    FROM tempTable
"""
duckdb.sql(max_query).show()
``` 

## Clip and compare Google V3 with merged dataset

Load a AOI GeoJSON as DuckDB table
```python
aoi = "path/to/aoi.geojson"
duckdb.sql(f"CREATE TABLE aoi AS SELECT * FROM ST_Read('{aoi}')")
duckdb.sql("SELECT * FROM aoi").show()
```

```python
# Load Lesotho from the merged dataset as a table
duckdb.sql(f"CREATE TABLE merged_bfs AS SELECT * FROM '{prefix}/by_country/country_iso={country_iso}/{country_iso}.parquet'")
duckdb.sql("SELECT * FROM merged_bfs").show()

# Load Lesotho from Google V3 as a table
prefix = "s3://us-west-2.opendata.source.coop/google-research-open-buildings/geoparquet-by-country"
country_iso = "LSO"
duckdb.sql(f"CREATE TABLE google_bfs AS SELECT * FROM '{prefix}/country_iso={country_iso}/{country_iso}.parquet'")
```
We now compare both tables to check for discrepancies in count value

# Total number of Google buildings
```python
duckdb.sql("SELECT COUNT(*) FROM google_bfs").show()
```

```python
# Total number of Google buildings in the merged dataset
duckdb.sql("SELECT COUNT(*) FROM merged_bfs WHERE bf_source = 'google'").show()
```