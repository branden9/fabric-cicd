'''Pass the required SPN values directly into the credential object, does not require AZ PowerShell or AZ CLI'''

from pathlib import Path

from azure.identity import ClientSecretCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# Assumes your script is one level down from root
root_directory = Path(__file__).resolve().parent

# Sample values for FabricWorkspace parameters, setup for dev
workspace_id = "c8136ebf-cce8-49be-9ca7-5d10b865e0ad"
environment = "dev"
repository_directory = str(root_directory / "sample/workspace")
item_type_in_scope = ["Notebook", "Environment"]  # List of item types to publish/unpublish

##test

# Use Azure CLI credential to authenticate
client_id = "2f757062-5312-4fdf-9a1d-1fa15fb5ac57" ##SPN App (client) ID
client_secret = "8if8Q~_7UM5jcyA8LuB-cuv8jGj95llRGtSUqa0u" ##SPN client secret
tenant_id = "e39cce29-5716-43ba-b27d-1bdd8fd67901" ##Tenant ID
token_credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)


# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    environment=environment,
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
    token_credential=token_credential,
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)
