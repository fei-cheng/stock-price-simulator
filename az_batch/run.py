import os
import shutil

from az_utils import blob_service_client as bsc
from az_utils import blobs_to_df, create_container, upload_file_to_container
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from batch import create_job, create_pool, run_job
from dotenv import load_dotenv

from stock_price_simulator.ticker import download_ticker_prices


def run():
    # set env
    load_dotenv()

    input_container_name = os.environ["input_container"]
    output_container_name = os.environ["output_container"]
    application_container_name = os.environ["application_container"]

    print("Creating resource files...")

    def clear_dir(dir):
        files = [f for f in os.listdir(dir) if f != ".gitignore"]
        for f in files:
            os.remove(os.path.join(dir, f))

    input_dir = "az_batch/data/input"
    output_dir = "az_batch/data/output"
    resource_dir = "az_batch/node/resources"
    package_name = "stock-price-simulator-0.1.0.tar.gz"

    print("clear directories...")
    clear_dir(input_dir)
    clear_dir(output_dir)
    clear_dir(resource_dir)

    # package code and then copy the package to the node folder
    os.system("poetry build --format sdist > /dev/null")
    shutil.copy(f"./dist/{package_name}", resource_dir)

    # download ticker data
    ticker_price_df = download_ticker_prices()
    for (ticker, price_df) in ticker_price_df.items():
        price_df.to_csv(os.path.join(input_dir, f"{ticker}.csv"), index=False)

    blob_service_client = bsc()
    # Use the blob client to create the containers in Azure Storage
    create_container(blob_service_client, input_container_name)
    create_container(blob_service_client, output_container_name)
    create_container(blob_service_client, application_container_name)

    input_files = []
    for folder, _, files in os.walk(input_dir):
        for filename in files:
            if filename.startswith("."):
                continue  # ignore hidden files
            filepath = os.path.join(folder, filename)
            input_files.append(
                upload_file_to_container(
                    blob_service_client, input_container_name, filepath
                )
            )

    application_files = []
    for folder, _, files in os.walk("az_batch/node"):
        for filename in files:
            if filename.startswith("."):
                continue  # ignore hidden files
            filepath = os.path.join(folder, filename)
            application_files.append(
                upload_file_to_container(
                    blob_service_client, application_container_name, filepath
                )
            )

    batch_service_client = BatchServiceClient(
        credentials=SharedKeyCredentials(
            os.environ["BATCH_ACCOUNT_NAME"], os.environ["BATCH_ACCOUNT_KEY"]
        ),
        batch_url=os.environ["BATCH_ACCOUNT_URL"],
    )

    pool_id = create_pool(batch_service_client, application_files, package_name)
    job_id = create_job(batch_service_client, pool_id)

    run_job(batch_service_client, input_files, pool_id, job_id)

    simulated_price_df = blobs_to_df(
        blob_service_client, output_container_name, prefix="data"
    )
    output_filepath = os.path.join(output_dir, "result.csv")
    simulated_price_df.to_csv(output_filepath, index=False)
    print(f"The simulation result saved in: {output_filepath}")


if __name__ == "__main__":
    run()
