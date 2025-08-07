import pandas as pd
import geopandas as gpd
from shapely import wkb
from scipy.spatial import cKDTree
import numpy as np


# MUNICIPAL BORDERS LAYER

df = pd.read_parquet("data/nordic_polygons.parquet")
df["geometry"] = df["geometry"].apply(wkb.loads)
poly_df = gpd.GeoDataFrame(df, geometry="geometry")


# flat longitude-latitude pair for the polygon layer
def flatten_geometry(geom):
    if geom.is_empty:
        return []
    elif geom.geom_type == "Polygon":
        return [list(coord)[:2] for coord in geom.exterior.coords]
    elif geom.geom_type == "MultiPolygon":
        return [
            [list(coord)[:2] for coord in poly.exterior.coords] for poly in geom.geoms
        ]
    else:
        raise ValueError("check geometry type: {}".format(geom.geom_type))


# borders = [flatten_geometry(geom) for geom in df["geometry"]]
# borders_str = f"MUN_BORDERS = {borders}"

# with open("data/nordic_polygons_flat.txt", "w") as file:
#     file.write(borders_str)


# POPULATION AND TYPOLOGY GRID LAYER

DATA = "data/nordic_points_1km.parquet"  # nordic grid
df = pd.read_parquet(DATA)
df["geometry"] = df["geometry"].apply(lambda wkb_data: wkb.loads(wkb_data))

gdf = gpd.GeoDataFrame(df, geometry="geometry")
gdf.set_crs("EPSG:4326", inplace=True)
gdf.rename(columns={
    # "jan17": "population2017",
    # "jan22": "population2022",
    "UrbRurTyp":"nordic_type",
}, inplace=True)

gdf_points = gdf[["country", "nordic_type", "geometry"]]
gdf_polygons = gpd.read_file("data/nord_mun22_lcc/nord_mun22_lcc.shp").to_crs(
    "EPSG:4326"
)
gdf_points = gdf_points.to_crs(gdf_polygons.crs)
gdf = gpd.sjoin(gdf_points, gdf_polygons, how="left", predicate="intersects")
gdf["muncode"] = gdf["COD_MUN"]


# get the nearest municipality name
points_without_municipality = gdf[gdf["muncode"].isna()]
points_with_municipality = gdf[gdf["muncode"].notna()]

coords_with_names = np.array(
    points_with_municipality.geometry.apply(lambda geom: (geom.x, geom.y)).tolist()
)
coords_without_names = np.array(
    points_without_municipality.geometry.apply(lambda geom: (geom.x, geom.y)).tolist()
)

tree = cKDTree(coords_with_names)
distances, indices = tree.query(coords_without_names, k=1)
points_without_municipality["muncode"] = points_with_municipality.iloc[
    indices.flatten()
]["muncode"].values

gdf_updated = pd.concat(
    [points_with_municipality, points_without_municipality], ignore_index=True
)
gdf = gdf_updated[
    ["nordic_type", "muncode", "country", "geometry"]
]

# add mun names
gdf_polygons = gdf_polygons.drop("geometry", axis=1)
df = gdf.merge(gdf_polygons, left_on="muncode", right_on="COD_MUN", how="left")
df["munname"] = df["MUN_NORDIC"]
df = df.drop_duplicates(subset=["geometry"])

df = df[
    [
        "nordic_type",
        "muncode",
        "munname",
        "geometry",
    ]
]

df.to_parquet("data/nordic_points.geoparquet", index=False)

print(df.info())
print(df)
