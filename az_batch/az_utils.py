import datetime
import os
from io import BytesIO

import azure.batch.models as batchmodels
import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import (
    BlobSasPermissions,
    BlobServiceClient,
    generate_blob_sas,
    generate_container_sas,
)


def blob_service_client():
    return BlobServiceClient(
        account_url=f"https://{os.environ['STORAGE_ACCOUNT_NAME']}.blob.core.windows.net/",
        credential=os.environ["STORAGE_ACCOUNT_KEY"],
    )


def create_container(blob_service_client, container_name, exist_ok=False):
    try:
        blob_service_client.create_container(container_name)
        print(f"Container [{container_name}] is created")
    except ResourceExistsError:
        if not exist_ok:
            print(f"Container [{container_name}] already exists, clear it...")
            clear_container(blob_service_client, container_name)


def clear_container(blob_service_client, container_name):
    # see https://stackoverflow.com/a/64747943
    container_client = blob_service_client.get_container_client(container_name)
    blobs = [blob for blob in container_client.list_blobs()]
    blobs_length = len(blobs)
    if blobs_length > 0:
        step = 256
        for i in range(0, blobs_length, step):
            container_client.delete_blobs(
                *blobs[i : i + step], delete_snapshots="include"
            )


def upload_file_to_container(blob_service_client, container_name, file_path):
    """
    Uploads a local file to an Azure Blob storage container.

    :param blob_service_client: A blob service client.
    :type blob_service_client: `azure.storage.blob.BlobServiceClient`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    print("Uploading file {} to container [{}]...".format(file_path, container_name))

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    account_name = os.environ["STORAGE_ACCOUNT_NAME"]
    account_key = os.environ["STORAGE_ACCOUNT_KEY"]
    account_domain = "blob.core.windows.net"

    sas_token = generate_blob_sas(
        account_name,
        container_name,
        blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=6),
    )

    sas_url = generate_sas_url(
        account_name,
        account_domain,
        container_name,
        blob_name,
        sas_token,
    )

    return batchmodels.ResourceFile(http_url=sas_url, file_path=blob_name)


def generate_sas_url(
    account_name, account_domain, container_name, blob_name, sas_token
):
    return "https://{}.{}/{}/{}?{}".format(
        account_name, account_domain, container_name, blob_name, sas_token
    )


def make_container_sas_url(container_name):
    account_domain = "blob.core.windows.net"

    container_sas = generate_container_sas(
        os.environ["STORAGE_ACCOUNT_NAME"],
        container_name,
        os.environ["STORAGE_ACCOUNT_KEY"],
        permission=BlobSasPermissions(write=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=5),
    )
    return f"https://{os.environ['STORAGE_ACCOUNT_NAME']}.{account_domain}/{container_name}?{container_sas}"


def blobs_to_df(blob_service_client, container_name, prefix=None):
    container_client = blob_service_client.get_container_client(container_name)

    dfs = []
    for blob in container_client.list_blobs(name_starts_with=prefix):
        blob_data = container_client.download_blob(blob.name).readall()
        df = pd.read_csv(BytesIO(blob_data))
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)
