import datetime
import os
import sys
import time
from typing import List, Set

import azure.batch.models as batchmodels
from az_utils import make_container_sas_url
from azure.batch import BatchServiceClient


def create_pool(batch_service_client, resource_files, package_name):
    pool_id = os.environ["POOL_ID"]
    pool_vm_size = os.environ["POOL_VM_SIZE"]
    pool_node_count = os.environ["POOL_NODE_COUNT"]

    print(f"Creating pool [{pool_id}]...")

    task_commands = [
        # Copy the task.py script to the "shared" directory
        # that all tasks that run on the node have access to. Note that
        # we are using the -p flag with cp to preserve the file uid/gid,
        # otherwise since this start task is run as an admin, it would not
        # be accessible by tasks run as a non-admin user.
        "sudo apt-get -y update",
        "sudo dpkg --configure -a",
        "sudo apt-get install -y python3-pip",
        "pip3 install --upgrade pip",
        f"sudo pip3 install {package_name}",
    ]
    for f in resource_files:
        task_commands.append(f"cp -p {f.file_path} $AZ_BATCH_NODE_SHARED_DIR")

    user = batchmodels.AutoUserSpecification(
        scope=batchmodels.AutoUserScope.pool,
        elevation_level=batchmodels.ElevationLevel.admin,
    )

    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="canonical",
                offer="0001-com-ubuntu-server-focal",
                sku="20_04-lts",
                version="latest",
            ),
            node_agent_sku_id="batch.node.ubuntu 20.04",
        ),
        vm_size=pool_vm_size,
        target_dedicated_nodes=pool_node_count,
        start_task=batchmodels.StartTask(
            command_line=wrap_commands_in_shell("linux", task_commands),
            user_identity=batchmodels.UserIdentity(auto_user=user),
            wait_for_success=True,
            resource_files=resource_files,
        ),
    )

    batch_service_client.pool.add(new_pool)

    start_time = datetime.datetime.now().replace(microsecond=0)
    # because we want all nodes to be available before any tasks are assigned
    # to the pool, here we will wait for all compute nodes to reach idle
    nodes = wait_for_all_nodes_state(
        batch_service_client,
        pool_id,
        frozenset(
            (
                batchmodels.ComputeNodeState.start_task_failed,
                batchmodels.ComputeNodeState.unusable,
                batchmodels.ComputeNodeState.idle,
            )
        ),
    )
    # ensure all node are idle
    if any(node.state != batchmodels.ComputeNodeState.idle for node in nodes):
        raise RuntimeError(f"node(s) of pool {pool_id} not in idle state")
    else:
        end_time = datetime.datetime.now().replace(microsecond=0)
        print(f"It takes {end_time - start_time} to create pool {pool_id}")

    return pool_id


def create_job(batch_service_client, pool_id):
    job_id = os.environ["JOB_ID"]
    print(f"Creating job [{job_id}]...")

    job = batchmodels.JobAddParameter(
        id=job_id, pool_info=batchmodels.PoolInformation(pool_id=pool_id)
    )

    batch_service_client.job.add(job)

    return job_id


def run_job(batch_service_client, input_files, pool_id, job_id):
    start_time = datetime.datetime.now().replace(microsecond=0)
    container_url = make_container_sas_url(os.environ["output_container"])

    try:
        # Add the tasks to the job. Pass the input files and a SAS URL
        # to the storage container for output files.
        print(f"Adding {len(input_files)} tasks to job [{job_id}]...")

        tasks = []
        for input_file in input_files:
            filepath = input_file.file_path
            ticker = os.path.splitext(os.path.basename(filepath))[0]
            command = f'/bin/bash -c "python3 $AZ_BATCH_NODE_SHARED_DIR/task.py --ticker {ticker} --filepath {filepath}"'

            tasks.append(
                batchmodels.TaskAddParameter(
                    id=ticker,
                    command_line=command,
                    resource_files=[input_file],
                    # environment_settings=[],
                    output_files=[
                        batchmodels.OutputFile(
                            file_pattern="../*.txt",
                            destination=batchmodels.OutputFileDestination(
                                container=batchmodels.OutputFileBlobContainerDestination(
                                    container_url=container_url,
                                    path=f"logs",
                                )
                            ),
                            upload_options=batchmodels.OutputFileUploadOptions(
                                upload_condition=batchmodels.OutputFileUploadCondition.task_completion
                            ),
                        ),
                        batchmodels.OutputFile(
                            file_pattern="data/output/*.csv",
                            destination=batchmodels.OutputFileDestination(
                                container=batchmodels.OutputFileBlobContainerDestination(
                                    container_url=container_url,
                                    path=f"data",
                                )
                            ),
                            upload_options=batchmodels.OutputFileUploadOptions(
                                upload_condition=batchmodels.OutputFileUploadCondition.task_success
                            ),
                        ),
                    ],
                )
            )

        batch_service_client.task.add_collection(job_id, tasks)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_succeed(batch_service_client, job_id)
        print(f"Deleting job [{job_id}]...")
        batch_service_client.job.delete(job_id)
        print(f"Deleting pool [{pool_id}]...")
        batch_service_client.pool.delete(pool_id)
    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise
    finally:
        # Print out some timing info
        print()
        end_time = datetime.datetime.now().replace(microsecond=0)
        print(f"Runtime: {end_time - start_time}")


def wrap_commands_in_shell(ostype, commands):
    """Wrap commands in a shell

    :param list commands: list of commands to wrap
    :param str ostype: OS type, linux or windows
    :rtype: str
    :return: a shell wrapping commands
    """
    if ostype.lower() == "linux":
        return '/bin/bash -c "{}"'.format(" && ".join(commands))
    elif ostype.lower() == "windows":
        return 'cmd.exe /c "{}"'.format("&".join(commands))
    else:
        raise ValueError("unknown ostype: {}".format(ostype))


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print("-------------------------------------------")
    print("Exception encountered:")
    if (
        batch_exception.error
        and batch_exception.error.message
        and batch_exception.error.message.value
    ):
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print("{}:\t{}".format(mesg.key, mesg.value))
    print("-------------------------------------------")


def wait_for_all_nodes_state(
    batch_client: BatchServiceClient,
    pool_id: str,
    node_state: Set[batchmodels.ComputeNodeState],
) -> List[batchmodels.ComputeNode]:
    """Waits for all nodes in pool to reach any specified state in set
    :param batch_client: The batch client to use.
    :param pool: The pool containing the node.
    :param node_state: node states to wait for
    :return: list of compute nodes
    """
    print(
        "Waiting for all pool nodes to be ready...",
        end="",
    )
    while True:
        # refresh pool to ensure that there is no resize error
        pool = batch_client.pool.get(pool_id)

        if pool.resize_errors is not None:
            resize_errors = "\n".join([repr(e) for e in pool.resize_errors])
            raise RuntimeError(
                f"resize error encountered for " f"pool {pool.id}:\n{resize_errors}"
            )
        nodes = list(batch_client.compute_node.list(pool.id))
        if len(nodes) >= pool.target_dedicated_nodes and all(
            node.state in node_state for node in nodes
        ):
            print()
            return nodes

        time.sleep(10)
        print(".", end="")
        sys.stdout.flush()


def wait_for_tasks_to_succeed(
    batch_client: BatchServiceClient,
    job_id: str,
):
    """
    Returns when all tasks in the specified job reach the succeeded state.
    """
    print("Waiting for all job tasks to be succeeded...", end="")

    while True:
        print(".", end="")
        sys.stdout.flush()
        tasks = batch_client.task.list(job_id)
        incomplete_tasks = [
            task for task in tasks if task.state != batchmodels.TaskState.completed
        ]
        if not incomplete_tasks:
            if batch_client.job.get_task_counts(job_id).task_counts.failed:
                raise RuntimeError("There are failed tasks, please check!")
            print()
            return True
        else:
            time.sleep(10)
