import duckdb
import time

duckdb.sql('INSTALL httpfs')
duckdb.sql('LOAD httpfs')
duckdb.sql('INSTALL spatial')
duckdb.sql('LOAD spatial')

# ----------------------------#
# S2 PARTITIONING AND TIMING  #
# ----------------------------#

# Load a full (large) country
prefix = "s3://us-west-2.opendata.source.coop/vida/google-microsoft-open-buildings/geoparquet"
country_iso = "IDN"

# Use single file
duckdb.sql(f"SELECT * FROM '{prefix}/by_country/country_iso={country_iso}/{country_iso}.parquet'").show()

# Use the s2 partitioning
duckdb.sql(f"SELECT * FROM parquet_scan('{prefix}/by_country_s2/country_iso={country_iso}/*.parquet')").show()

# TODO: show timing difference


# ----------------------------#
# COUNT BUILDINGS             #
# ----------------------------#
# Pick a smaller country
country_iso = "LSO"

# Count MS buildings
duckdb.sql(f"SELECT count(*) FROM parquet_scan('{prefix}/by_country_s2/country_iso={country_iso}/*.parquet') WHERE bf_source = 'microsoft'")
# ┌──────────────┐
# │ count_star() │
# │    int64     │
# ├──────────────┤
# │       151722 │
# └──────────────┘

# Count Google buildings
duckdb.sql(f"SELECT count(*) FROM parquet_scan('{prefix}/by_country_s2/country_iso={country_iso}/*.parquet') WHERE bf_source = 'google'")
# ┌──────────────┐
# │ count_star() │
# │    int64     │
# ├──────────────┤
# │      1394189 │
# └──────────────┘

# TODO: write something about the added 151722 MS buildings


# -----------------------------------------------#
# CLIP AND COMPARE GOOGLE V3 WITH MERGED DATASET #
# -----------------------------------------------#

# Load a AOI GeoJSON as DuckDB table
aoi = "path/to/aoi.geojson"
duckdb.sql(f"CREATE TABLE aoi AS SELECT * FROM ST_Read('{aoi}')")
duckdb.sql("SELECT * FROM aoi").show()

# Load Lesotho from the merged dataset as a table
duckdb.sql(f"CREATE TABLE merged_bfs AS SELECT * FROM '{prefix}/by_country/country_iso={country_iso}/{country_iso}.parquet'")
duckdb.sql("SELECT * FROM merged_bfs").show()

# Load Lesotho from Google V3 as a table
prefix = "s3://us-west-2.opendata.source.coop/google-research-open-buildings/geoparquet-by-country"
country_iso = "LSO"
duckdb.sql(f"CREATE TABLE google_bfs AS SELECT * FROM '{prefix}/country_iso={country_iso}/{country_iso}.parquet'")

# Compare complete dataset

# Total number of Google buildings
duckdb.sql("SELECT COUNT(*) FROM google_bfs").show()
# ┌──────────────┐
# │ count_star() │
# │    int64     │
# ├──────────────┤
# │      1394225 │
# └──────────────┘

# Total number of Google buildings in the merged dataset
duckdb.sql("SELECT COUNT(*) FROM merged_bfs WHERE bf_source = 'google'").show()
# ┌──────────────┐
# │ count_star() │
# │    int64     │
# ├──────────────┤
# │      1394189 │
# └──────────────┘

# TODO: could be interesting to describe this difference of 36 buildings due to geoboundaries used.

# Create subset

# Clip merged table
clip_query = """
CREATE TABLE clipped_merged_bfs AS
SELECT ST_Intersection(ST_GeomFromWKB(b.geometry), ST_GeomFromWKB(a.wkb_geometry)) AS geom, b.bf_source
FROM merged_bfs b, aoi a
WHERE ST_Intersects(ST_GeomFromWKB(b.geometry), ST_GeomFromWKB(a.wkb_geometry));
"""
duckdb.sql(clip_query)

# Clip google table
clip_query = """
CREATE TABLE clipped_google_bfs AS
SELECT ST_Intersection(ST_GeomFromWKB(b.geometry), ST_GeomFromWKB(a.wkb_geometry)) AS geom
FROM google_bfs b, aoi a
WHERE ST_Intersects(ST_GeomFromWKB(b.geometry), ST_GeomFromWKB(a.wkb_geometry));
"""
duckdb.sql(clip_query)


# TODO: could add image here of AOI, showing the Google and Microsoft buildings

# Compare original Google dataset with merged dataset
# Total number of Google buildings
duckdb.sql("SELECT COUNT(*) FROM clipped_google_bfs").show()
# ┌──────────────┐
# │ count_star() │
# │    int64     │
# ├──────────────┤
# │        13213 │
# └──────────────┘

# Total number of Google buildings in the merged dataset
duckdb.sql("SELECT COUNT(*) FROM clipped_merged_bfs WHERE bf_source = 'google'").show()
# ┌──────────────┐
# │ count_star() │
# │    int64     │
# ├──────────────┤
# │        13213 │
# └──────────────┘

# TODO: write about the fact that its the same number of buildings

# Now check the total added MS buildings
duckdb.sql("SELECT COUNT(*) FROM clipped_merged_bfs WHERE bf_source = 'microsoft'").show()
# ┌──────────────┐
# │ count_star() │
# │    int64     │
# ├──────────────┤
# │          348 │
# └──────────────┘

# Intersect both clipped tables and check that we do not have overlap
clip_query = """
CREATE TABLE intersected AS
SELECT m.*
FROM clipped_merged_bfs m
WHERE NOT EXISTS (
    SELECT 1
    FROM clipped_google_bfs g
    WHERE ST_Intersects(m.geom, g.geom)
);

"""
duckdb.sql(clip_query)
duckdb.sql("SELECT * FROM intersected").show()
duckdb.sql("SELECT count(*) FROM intersected").show()
# ┌──────────────┐
# │ count_star() │
# │    int64     │
# ├──────────────┤
# │          348 │
# └──────────────┘

# TODO: write about how this is the same number, meaning there is no Microsoft building intersecting with a Google building
duckdb.sql("SELECT count(*) FROM intersected WHERE bf_source = 'google'").show()
# ┌──────────────┐
# │ count_star() │
# │    int64     │
# ├──────────────┤
# │            0 │
# └──────────────┘
