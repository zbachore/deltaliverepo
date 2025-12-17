# Microsoft Fabric and Azure Key Vault Integration

## Overview

**Yes, Microsoft Fabric is integrated with Azure Key Vault.** Microsoft Fabric provides native integration with Azure Key Vault to securely store and manage secrets, connection strings, API keys, and other sensitive information used in your data pipelines and analytics workloads.

## What is Microsoft Fabric?

Microsoft Fabric is a unified analytics platform that brings together data integration, data engineering, data warehousing, data science, real-time analytics, and business intelligence into a single integrated environment. It's built on a foundation of OneLake, a unified data lake that serves as the single source of truth for all data.

## Key Vault Integration Features

### 1. Secure Secret Management
Microsoft Fabric allows you to reference secrets stored in Azure Key Vault instead of hardcoding sensitive information in your notebooks, pipelines, or configurations.

### 2. Supported Secret Types
- **Connection Strings**: Database connection strings, storage account keys
- **API Keys**: External service API keys and tokens
- **Credentials**: Usernames, passwords, and authentication tokens
- **Certificates**: SSL/TLS certificates for secure connections

### 3. Integration Points
Microsoft Fabric integrates with Azure Key Vault across multiple workload types:

- **Data Pipelines**: Reference Key Vault secrets in pipeline activities and linked services
- **Notebooks**: Access secrets programmatically within Spark notebooks
- **Dataflows**: Use secrets for data source connections
- **Real-time Analytics**: Secure connection strings for event streaming
- **Power BI**: Secure data source credentials

## How to Configure Key Vault Integration

### Prerequisites
1. An active Azure subscription
2. An Azure Key Vault instance
3. A Microsoft Fabric workspace
4. Appropriate permissions (Key Vault Secrets User role or higher)

### Step 1: Create Azure Key Vault
```bash
# Using Azure CLI
az keyvault create \
  --name "your-keyvault-name" \
  --resource-group "your-resource-group" \
  --location "eastus"
```

### Step 2: Store Secrets in Key Vault
```bash
# Add a secret to Key Vault
az keyvault secret set \
  --vault-name "your-keyvault-name" \
  --name "DatabaseConnectionString" \
  --value "Server=myserver.database.windows.net;Database=mydb;..."
```

### Step 3: Grant Fabric Access to Key Vault
Configure Azure Key Vault access policies or use Azure RBAC to grant Microsoft Fabric managed identity access to your Key Vault:

```bash
# Grant secret read permissions
az keyvault set-policy \
  --name "your-keyvault-name" \
  --object-id "<fabric-managed-identity-id>" \
  --secret-permissions get list
```

### Step 4: Reference Secrets in Fabric

#### In Notebooks (PySpark):
```python
# Using Azure Key Vault with Azure SDK
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Initialize Key Vault client
credential = DefaultAzureCredential()
vault_url = "https://your-keyvault-name.vault.azure.net/"
client = SecretClient(vault_url=vault_url, credential=credential)

# Retrieve a secret
secret = client.get_secret("DatabaseConnectionString")
connection_string = secret.value

# Use the connection string in your code
df = spark.read \
    .format("jdbc") \
    .option("url", connection_string) \
    .load()
```

#### In Data Pipelines:
When configuring linked services or datasets, you can reference Key Vault secrets using the Azure Key Vault linked service:

```json
{
  "type": "AzureKeyVaultSecret",
  "store": {
    "referenceName": "AzureKeyVaultLinkedService",
    "type": "LinkedServiceReference"
  },
  "secretName": "DatabaseConnectionString"
}
```

## Security Best Practices

### 1. Use Managed Identities
- Enable system-assigned or user-assigned managed identities for Microsoft Fabric
- Avoid storing credentials in code or configuration files

### 2. Implement Least Privilege Access
- Grant only the minimum required permissions to Key Vault
- Use Azure RBAC for fine-grained access control

### 3. Enable Key Vault Logging
- Enable diagnostic logging in Azure Key Vault
- Monitor secret access and changes

### 4. Secret Rotation
- Implement regular secret rotation policies
- Use Key Vault's built-in secret versioning

### 5. Network Security
- Configure Key Vault firewall rules
- Use private endpoints for secure access within your virtual network

## Comparison with Databricks

Both Microsoft Fabric and Azure Databricks provide integration with Azure Key Vault for secure secret management:

| Feature | Microsoft Fabric | Azure Databricks |
|---------|-----------------|------------------|
| Native Key Vault Integration | ✓ Yes | ✓ Yes |
| Managed Identity Support | ✓ Yes | ✓ Yes |
| Secret Scopes | Via Key Vault | Secret Scopes + Key Vault |
| Notebook Access | Azure SDK | dbutils.secrets |
| Pipeline Integration | Native | Via notebooks/jobs |
| Access Control | Azure RBAC | Databricks ACLs + Azure RBAC |

### Databricks Secret Management Example
For comparison, here's how you would access Key Vault secrets in Databricks:

```python
# Databricks approach using secret scopes
connection_string = dbutils.secrets.get(scope="key-vault-scope", key="DatabaseConnectionString")

# Use the connection string
df = spark.read \
    .format("jdbc") \
    .option("url", connection_string) \
    .load()
```

## Benefits of Using Key Vault with Microsoft Fabric

1. **Centralized Secret Management**: Manage all secrets in one secure location
2. **Compliance**: Meet regulatory requirements for secret storage and access
3. **Audit Trail**: Track who accessed which secrets and when
4. **Separation of Concerns**: Developers don't need direct access to secrets
5. **Secret Rotation**: Easily rotate secrets without updating code
6. **Multi-Environment Support**: Use different Key Vaults for dev, test, and production

## Common Use Cases

### 1. Database Connections
Store database connection strings securely and reference them in data pipelines and notebooks.

### 2. External API Integration
Securely manage API keys for third-party services used in data integration workflows.

### 3. Storage Account Access
Store storage account keys and SAS tokens for accessing Azure Storage.

### 4. Service Principal Credentials
Manage service principal client secrets for application authentication.

### 5. Certificate Management
Store SSL/TLS certificates for secure connections to external services.

## Troubleshooting

### Issue: Unable to Access Key Vault Secrets
**Solution**: Verify that:
- The Fabric managed identity has appropriate permissions on Key Vault
- The Key Vault firewall allows access from Fabric
- The secret name is correct and the secret exists

### Issue: Authentication Failures
**Solution**: 
- Ensure DefaultAzureCredential is properly configured
- Check that managed identity is enabled
- Verify Azure RBAC role assignments

### Issue: Secret Not Found
**Solution**:
- Confirm the secret exists in Key Vault
- Check the Key Vault URL is correct
- Verify secret name spelling and casing

## Additional Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Azure Key Vault Documentation](https://learn.microsoft.com/en-us/azure/key-vault/)
- [Azure Key Vault SDK for Python](https://learn.microsoft.com/en-us/python/api/overview/azure/keyvault-secrets-readme)
- [Best Practices for Azure Key Vault](https://learn.microsoft.com/en-us/azure/key-vault/general/best-practices)

## Conclusion

Microsoft Fabric's integration with Azure Key Vault provides a robust, secure, and scalable solution for managing secrets and sensitive information in your data analytics workloads. By leveraging this integration, organizations can ensure compliance, improve security posture, and simplify secret management across their data estate.
