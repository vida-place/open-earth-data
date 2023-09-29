# Google-Microsoft Open Buildings - DuckDB tutorial

This tutorial is built around the recently released [Google-Microsoft Open Buildings - combined by VIDA](https://beta.source.coop/repositories/vida/google-microsoft-open-buildings/description) dataset on [Source Cooperative](https://beta.source.coop/). We will show you how to access the cloud-native dataformats directly from Source Cooperative using [DuckDB](https://duckdb.org/). We will talk about the partition strategy, its implementations for performance and we will do some tests comparing the merged dataset with just the Google V3 building footprints, which is also hosted on [Source Cooperative](https://beta.source.coop/repositories/cholmes/google-open-buildings/description)

# Setting up DuckDB

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

# Loading GeoParquet directly from S3 to DuckDB

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

# Count the building footprints and check the source

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
