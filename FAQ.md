# Frequently Asked Questions (FAQ)

## Microsoft Fabric and Azure Integration

### Is Microsoft Fabric integrated with Azure Key Vault?

**Yes**, Microsoft Fabric is integrated with Azure Key Vault for secure credential and secrets management.

#### Key Integration Features:

1. **Secure Credential Storage**
   - Microsoft Fabric can securely store and retrieve connection strings, passwords, API keys, and other sensitive information from Azure Key Vault
   - This eliminates the need to hardcode credentials in notebooks, pipelines, or configuration files

2. **Integration Methods**

   **Using Azure Key Vault in Microsoft Fabric:**
   
   - **Linked Services**: Create linked connections to Azure Key Vault to reference secrets in your data pipelines and notebooks
   - **Key Vault References**: Use Key Vault secret references in connection strings and configuration settings
   - **Managed Identity**: Leverage Azure Managed Identity for secure, password-less authentication between Fabric and Key Vault

3. **Best Practices**

   - **Use Managed Identity**: Configure Microsoft Fabric workspaces to use Managed Identity for accessing Key Vault without storing credentials
   - **Least Privilege Access**: Grant only necessary permissions to Fabric applications when accessing Key Vault secrets
   - **Secret Rotation**: Implement automatic secret rotation in Key Vault to enhance security
   - **Audit Logging**: Enable Azure Key Vault logging to track secret access and maintain compliance

4. **Common Use Cases**

   - **Database Connections**: Store database connection strings securely in Key Vault and reference them in Fabric data pipelines
   - **API Authentication**: Securely manage API keys and tokens used in Fabric notebooks and data flows
   - **Third-Party Service Credentials**: Store credentials for external services (AWS, GCP, SaaS applications) that Fabric needs to access
   - **Certificate Management**: Store and manage SSL/TLS certificates used by Fabric applications

5. **Security Benefits**

   - **Centralized Secret Management**: All secrets are managed in one secure location
   - **Access Control**: Fine-grained RBAC (Role-Based Access Control) for secret access
   - **Encryption**: Secrets are encrypted at rest and in transit
   - **Compliance**: Helps meet regulatory requirements for secure credential management (GDPR, HIPAA, SOC 2)

#### Example Integration Flow:

```
1. Create an Azure Key Vault instance
2. Store your secrets (e.g., database connection string) in Key Vault
3. Configure Microsoft Fabric workspace with appropriate permissions to access Key Vault
4. In your Fabric notebook or pipeline, reference the Key Vault secret using the secret URI
5. Fabric retrieves the secret securely at runtime without exposing it in code
```

#### Additional Resources:

- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Azure Key Vault Documentation](https://learn.microsoft.com/en-us/azure/key-vault/)
- [Manage credentials in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/security/security-overview)

---

## Delta Live Tables Questions

### How do I configure Delta Live Tables with external parameters?

Delta Live Tables can be configured using pipeline settings. Pass parameters like `catalog_name`, `schema_name`, `volume_name`, and `num_records` through the pipeline configuration settings. These parameters can then be accessed in your notebooks using Spark configuration methods.

### Can I use different data sources with Delta Live Tables?

Yes, Delta Live Tables support multiple data sources including:
- JSON files (as demonstrated in this repository)
- CSV files
- Parquet files
- Delta tables
- Streaming sources via Auto Loader
- JDBC connections to databases

### What are SCD Type 1 and Type 2?

**SCD (Slowly Changing Dimension)** types are patterns for handling changes in dimension data:

- **Type 1**: Overwrites old data with new data. No history is maintained.
- **Type 2**: Maintains historical data by creating new rows with version identifiers or timestamps.

This repository includes notebooks demonstrating both SCD Type 1 and Type 2 implementations.

---

## Need More Help?

If you have additional questions or need assistance, please:
1. Check the [README.md](README.md) for getting started instructions
2. Review the [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines
3. Open an issue in the GitHub repository for specific questions or bug reports
