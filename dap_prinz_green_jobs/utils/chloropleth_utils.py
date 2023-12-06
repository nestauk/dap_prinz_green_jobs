import geopandas as gpd
import pandas as pd
from urllib.request import urlretrieve
from zipfile import ZipFile

import os


def get_nuts3polygons_dict():
    shape_url = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/download/ref-nuts-2021-20m.geojson.zip"
    nuts_file = "NUTS_RG_20M_2021_4326_LEVL_3.geojson"
    shapefile_path = "shapefiles/"

    full_shapefile_path = shapefile_path
    if not os.path.isdir(full_shapefile_path):
        os.mkdir(full_shapefile_path)

    zip_path, _ = urlretrieve(shape_url)
    with ZipFile(zip_path, "r") as zip_files:
        for zip_names in zip_files.namelist():
            if zip_names == nuts_file:
                zip_files.extract(zip_names, path=full_shapefile_path)
                nuts_geo = gpd.read_file(full_shapefile_path + nuts_file)
                nuts_geo = nuts_geo[nuts_geo["CNTR_CODE"] == "UK"].reset_index(
                    drop=True
                )
    nuts3polygons = nuts_geo[["NUTS_ID", "geometry", "NUTS_NAME"]]
    nuts3polygons.index = nuts3polygons["NUTS_ID"]
    nuts3polygons_dict = {
        k: (v["geometry"], v["NUTS_NAME"])
        for k, v in pd.DataFrame(nuts3polygons).to_dict(orient="index").items()
    }
    return nuts3polygons_dict


def get_nuts2polygons_dict():
    shape_url = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/download/ref-nuts-2021-20m.geojson.zip"
    nuts_file = "NUTS_RG_20M_2021_4326_LEVL_2.geojson"
    shapefile_path = "shapefiles/"

    full_shapefile_path = shapefile_path
    if not os.path.isdir(full_shapefile_path):
        os.mkdir(full_shapefile_path)

    zip_path, _ = urlretrieve(shape_url)
    with ZipFile(zip_path, "r") as zip_files:
        for zip_names in zip_files.namelist():
            if zip_names == nuts_file:
                zip_files.extract(zip_names, path=full_shapefile_path)
                nuts_geo = gpd.read_file(full_shapefile_path + nuts_file)
                nuts_geo = nuts_geo[nuts_geo["CNTR_CODE"] == "UK"].reset_index(
                    drop=True
                )
    nuts2polygons = nuts_geo[["NUTS_ID", "geometry", "NUTS_NAME"]]
    nuts2polygons.index = nuts2polygons["NUTS_ID"]
    nuts2polygons_dict = {
        k: (v["geometry"], v["NUTS_NAME"])
        for k, v in pd.DataFrame(nuts2polygons).to_dict(orient="index").items()
    }
    return nuts2polygons_dict


def get_nuts1polygons_dict():
    shape_url = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/download/ref-nuts-2021-20m.geojson.zip"
    nuts_file = "NUTS_RG_20M_2021_4326_LEVL_1.geojson"
    shapefile_path = "shapefiles/"

    full_shapefile_path = shapefile_path
    if not os.path.isdir(full_shapefile_path):
        os.mkdir(full_shapefile_path)

    zip_path, _ = urlretrieve(shape_url)
    with ZipFile(zip_path, "r") as zip_files:
        for zip_names in zip_files.namelist():
            if zip_names == nuts_file:
                zip_files.extract(zip_names, path=full_shapefile_path)
                nuts_geo = gpd.read_file(full_shapefile_path + nuts_file)
                nuts_geo = nuts_geo[nuts_geo["CNTR_CODE"] == "UK"].reset_index(
                    drop=True
                )
    nuts1polygons = nuts_geo[["NUTS_ID", "geometry", "NUTS_NAME"]]
    nuts1polygons.index = nuts1polygons["NUTS_ID"]
    nuts1polygons_dict = {
        k: (v["geometry"], v["NUTS_NAME"])
        for k, v in pd.DataFrame(nuts1polygons).to_dict(orient="index").items()
    }
    return nuts1polygons_dict
