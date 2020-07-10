import os
import csv
import numpy as np
from pandas import read_excel
import requests
import supervisely_lib as sly
import pathlib
from google.cloud import storage
# pip install google-cloud-storage

# TEAM_ID = int('%%TEAM_ID%%') # current team_id
# WORKSPACE_ID = int('%%WORKSPACE_ID%%') # current user's workspace_id (optional field, can be ignored in many cases)
# FILE_PATH = sly.script.read_str_input('%%FILE_PATH%%')


TEAM_ID = 4
WORKSPACE_ID = 14

DIRECTORY_PATH = sly.script.read_str_input('"/favorita_import_01"')

api = sly.Api.from_env()

# Validate parameters
team = api.team.get_info_by_id(TEAM_ID)
if team is None:
    raise RuntimeError('Team with id={!r} not found'.format(TEAM_ID))

workspace = api.workspace.get_info_by_id(WORKSPACE_ID)
if workspace is None:
    raise RuntimeError('Workspace with id={!r} not found'.format(WORKSPACE_ID))

links_remote_path = os.path.join(DIRECTORY_PATH, "links.csv")
links_local_path = os.path.join("/sly_task_data/links.csv")
sly.fs.ensure_base_path(links_local_path)

catalog_remote_path = os.path.join(DIRECTORY_PATH, "product_catalog.xlsx")
catalog_local_path = os.path.join("/sly_task_data/product_catalog.xlsx")
sly.fs.ensure_base_path(catalog_local_path)

gs_key_remote_path = os.path.join(DIRECTORY_PATH, "gs_key.json")
gs_key_local_path = os.path.join("/sly_task_data/gs_key.json")
sly.fs.ensure_base_path(gs_key_local_path)

api.file.download(team.id, links_remote_path, links_local_path)
api.file.download(team.id, catalog_remote_path, catalog_local_path)
api.file.download(team.id, gs_key_remote_path, gs_key_local_path)

# to get gs_key: https://cloud.google.com/docs/authentication/getting-started
# https://cloud.google.com/storage/docs/downloading-objects#storage-download-object-python
storage_client = storage.Client.from_service_account_json(gs_key_local_path)


links = None
with open(links_local_path) as f:
    links = list(csv.reader(f))
sly.logger.info("Number of links: {}".format(len(links)))

sheets = read_excel(catalog_local_path, sheet_name=None)
catalog = sheets[list(sheets.keys())[0]] # get first sheet from excel
sly.logger.info("Size of catalog: {}".format(len(catalog)))


structure = {} # workspace_name -> project_name -> dataset_name -> item_name ->
upc_to_subcategory = {}
for link in links:
    gcp_path = link[0]
    upc_code_debug = link[1]

    gcp_path = gcp_path.replace("gs://", "https://storage.cloud.google.com/")
    upc_folder = os.path.dirname(gcp_path)
    upc_code = os.path.basename(upc_folder)

    project_dir = os.path.dirname(upc_folder)
    project_name = os.path.basename(project_dir)  # "articulo" or "percha"

    category_dir = os.path.dirname(project_dir)
    categoty_name = os.path.basename(category_dir)  # for each category will be created separate workspace

    if upc_code_debug != upc_code:
        raise RuntimeError("UPC code from CSV != UPC code from path ({!r} != {!r})".format(upc_code_debug, upc_code))

    if categoty_name not in structure:
        structure[categoty_name] = {}

    if project_name not in structure[categoty_name]:
        structure[categoty_name][project_name] = {}

    if upc_code not in structure[categoty_name][project_name]:
        structure[categoty_name][project_name][upc_code] = []

        res = catalog[catalog['UPC CODE'] == np.int64(upc_code)]
        subcategory = "unknown"
        if len(res) != 1:
            sly.logger.warn("Unknowns subcategory for upc_code = {!r}".format(upc_code))
        else:
            subcategory = res.iloc[0]["SUB-CATEGORY SPANISH"]
        upc_to_subcategory[upc_code] = subcategory

    structure[categoty_name][project_name][upc_code].append(gcp_path)


def download_gcp_image(remote_path, local_path):
    p = pathlib.Path(remote_path)
    bucket_name = p.parts[2]
    source_blob_name = os.path.join(*p.parts[3:])

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(local_path)


progress = sly.Progress("Processing", len(links))
for category_name, projects in structure.items():
    for project_name, datasets in projects.items():
        for upc_code, links in datasets.items():
            # upload data to platform
            workspace = api.workspace.get_info_by_name(team.id, categoty_name)
            if workspace is None:
                workspace = api.workspace.create(team.id, categoty_name)

            project = api.project.get_info_by_name(workspace.id, project_name)
            if project is None:
                project = api.project.create(workspace.id, project_name)

            dataset_name = "{}_{}".format(upc_to_subcategory[upc_code], upc_code)
            dataset = api.dataset.get_info_by_name(project.id, dataset_name)
            if dataset is None:
                dataset = api.dataset.create(project.id, dataset_name)

            # it's used to skip already uploaded images if the script is crashed and we start it again
            existing_images = set()
            dataset_images = api.image.get_list(dataset.id)
            for image_info in dataset_images:
                existing_images.add(image_info.name)

            for batch in sly.batched(links):
                # api.image.upload_links is not suitable, because link can become unavailable + need GCP authorization
                image_names = []
                image_paths = []
                for link in batch:
                    image_name = sly.fs.get_file_name_with_ext(link)
                    if image_name in existing_images:
                        progress.iter_done_report()
                        continue

                    image_names.append(image_name)

                    image_local_path = os.path.join("/sly_task_data/temp", image_name)
                    image_paths.append(image_local_path)
                    sly.fs.ensure_base_path(image_local_path)

                    download_gcp_image(link, image_local_path)
                    
                api.image.upload_paths(dataset.id, image_names, image_paths, progress.iters_done_report)
                for image_path in image_paths:
                    sly.fs.silent_remove(image_path)

sly.logger.info("Import Finished")

# https://cloud.google.com/docs/authentication/getting-started#cloud-console
# export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/my-key.json"
