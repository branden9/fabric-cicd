# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Example to set variables based on the target environment.
Environment is determined based on the current branch name.
"""

from pathlib import Path

import git  # Depends on pip install gitpython

from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# Grab the current branch name that is checked out
repo = git.Repo(search_parent_directories=True)
repo.remotes.origin.pull()
branch = repo.active_branch.name


# The defined environment values should match the names found in the parameter.yml file
if branch == "dev":
    workspace_id = "c8136ebf-cce8-49be-9ca7-5d10b865e0ad"
    environment = "DEV"
elif branch == "test":
    workspace_id = "13812bb7-8840-4dfd-8b79-04976880b97f"
    environment = "TEST"
elif branch == "main":
    workspace_id = "9010397b-7c0f-4d93-8620-90e51816e9e9"
    environment = "PROD"
else:
    raise ValueError("Invalid branch to deploy from")

# # Sample values for FabricWorkspace parameters
repository_directory = "sample\workspace"
item_type_in_scope = ["Notebook", "Environment"]
# item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    environment=environment,
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)
