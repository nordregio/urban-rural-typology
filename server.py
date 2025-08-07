from flask import Flask, send_from_directory
import pandas as pd
import geopandas as gpd
from shapely import wkb
import ast
import json

# MUNICIPAL BORDERS LAYER
with open("data/nordic_polygons_flat.txt", "r") as file:
    content = file.read()

borders = ast.literal_eval(content.split("= ")[1])

# POPULATION GRID LAYER
DATA = "data/nordic_points.geoparquet"
df = pd.read_parquet(DATA)
df["geometry"] = df["geometry"].apply(wkb.loads)
df = gpd.GeoDataFrame(df, geometry="geometry")
df["lng"] = df["geometry"].x
df["lat"] = df["geometry"].y

df = df[
    [
        "lat",
        "lng",
        "nordic_type",
        "munname",
    ]
]


def assign_color(nordic_type):
    if nordic_type == "Sparsely populated rural area":
        return [105, 143, 92, 255]  # #698f5c
    elif nordic_type == "Rural heartland":
        return [163, 191, 124, 255]  # #a3bf7c
    elif nordic_type == "Rural area close to urban":
        return [194, 216, 170, 255]  # #c2d8aa
    elif nordic_type == "Local centre in rural area":
        return [221, 196, 171, 255]  # #ddc4ab
    elif nordic_type == "Peri-urban area":
        return [203, 166, 124, 255]  # #cba67c
    elif nordic_type == "Outer urban area":
        return [165, 109, 52, 255]  # #a56d34
    elif nordic_type == "Inner urban area":
        return [104, 62, 12, 255]  # #673e0c
    else:
        return [112, 111, 111, 255]  # #706f6f


df["fill_color"] = df["nordic_type"].apply(assign_color)

app = Flask(__name__)


@app.route("/save")
def save():
    with open("layers/polygon_layer.json", "w") as h:
        h.write(json.dumps(borders))

    with open("layers/column_layer.json", "w") as h:
        h.write(df.to_json(orient="records"))

    return "done"


@app.route("/layers/<path:name>")
def serve_layers(name):
    return send_from_directory("layers", name)


@app.route("/")
def index():
    with open("index.html", "r") as h:
        return h.read()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, use_reloader=True, debug=True)
