# Deploying to VNet-Injected Azure Databricks with GitHub Actions

This guide explains how to deploy Delta Live Tables pipelines to a VNet-injected (private network) Azure Databricks workspace using GitHub Actions and Databricks Asset Bundles (DAB).

## Overview

VNet-injected Databricks workspaces are deployed within an Azure Virtual Network (VNet) for enhanced security and network isolation. These workspaces are not directly accessible from the public internet, requiring special configuration for CI/CD deployments.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  GitHub Actions (Self-Hosted Runner)                │
│  Running in Azure VNet or with VPN/ExpressRoute     │
│  - Has network access to private Databricks         │
│  - Executes databricks bundle commands              │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ Private Network Connection
                   │ (VNet Peering, Private Endpoint, etc.)
                   ▼
┌─────────────────────────────────────────────────────┐
│  VNet-Injected Azure Databricks Workspace           │
│  - Private subnet for Databricks resources          │
│  - No public IP addresses                           │
│  - Accessed via Private Endpoint                    │
└─────────────────────────────────────────────────────┘
```

## Prerequisites

### 1. Self-Hosted GitHub Runner Setup

For VNet-injected Databricks, you **must** use a self-hosted GitHub runner that can access your private network. Options include:

- **Option A**: Deploy runner in the same VNet as Databricks
- **Option B**: Deploy runner with VPN connection to the VNet
- **Option C**: Use Azure ExpressRoute for hybrid connectivity

#### Setting up Self-Hosted Runner in Azure

```bash
# Example: Create a VM in the same VNet as Databricks
az vm create \
  --resource-group <your-rg> \
  --name github-runner \
  --vnet-name <databricks-vnet> \
  --subnet <runner-subnet> \
  --image UbuntuLTS \
  --admin-username azureuser \
  --generate-ssh-keys

# SSH into the VM and install GitHub runner
# Follow: https://docs.github.com/en/actions/hosting-your-own-runners/adding-self-hosted-runners
```

#### Configure Runner Labels

Add the label `databricks-vnet` to your self-hosted runner:
1. Go to repository Settings → Actions → Runners
2. Edit your runner
3. Add label: `databricks-vnet`

### 2. Network Configuration

Ensure your runner can reach the Databricks workspace:

- **Private Endpoint**: Configure Azure Private Endpoint for Databricks
- **DNS Resolution**: Ensure private DNS zones are configured
- **NSG Rules**: Allow outbound connections to Databricks control plane
- **Firewall Rules**: Allow access to required endpoints

#### Test Connectivity

```bash
# On your self-hosted runner, test connectivity
curl -I https://<your-databricks-workspace>.azuredatabricks.net

# Should return 200 OK or redirect
```

### 3. GitHub Secrets Configuration

Configure these secrets in your repository (Settings → Secrets and variables → Actions):

#### For Dev Environment
- `DATABRICKS_HOST`: Full workspace URL (e.g., `https://adb-xxxxx.azuredatabricks.net`)
- `DATABRICKS_TOKEN`: Personal Access Token or Service Principal token

#### For QA Environment
- `DATABRICKS_HOST`: QA workspace URL
- `DATABRICKS_TOKEN`: QA workspace token

#### For Prod Environment
- `DATABRICKS_HOST`: Prod workspace URL
- `DATABRICKS_TOKEN`: Prod workspace token

#### Authentication Options

**Option 1: Personal Access Token (PAT)**
```bash
# In Databricks workspace:
# User Settings → Developer → Access Tokens → Generate New Token
```

**Option 2: Service Principal (Recommended for Production)**
```bash
# Create Azure AD Service Principal
az ad sp create-for-rbac --name databricks-cicd-sp

# Add SP to Databricks workspace as admin
# Databricks → Settings → Identity & Access → Service Principals
```

## Databricks Asset Bundle Configuration

The deployment uses Databricks Asset Bundles (DAB) defined in `asset-bundle/databricks.yaml`:

### Bundle Structure

```yaml
bundle:
  name: deltaliverepo

resources:
  pipelines:
    # DLT Pipelines defined here
    pipeline_dlt_pipeline_configuration_driven: ...
    pipeline_dlt_scd_demo: ...

targets:
  dev:    # Development environment
  qa:     # QA environment  
  prod:   # Production environment
```

### Key Features

- **Environment-specific configurations**: Different catalogs, volumes, and parameters per environment
- **Serverless compute**: Uses Databricks serverless for cost efficiency
- **Photon acceleration**: Enabled for better performance
- **Source control**: All notebooks deployed from Git repository

## Workflow Usage

### Manual Deployment

The workflow is triggered manually via GitHub Actions UI:

1. Go to **Actions** tab in your repository
2. Select **Deploy to VNet-Injected Azure Databricks using DAB**
3. Click **Run workflow**
4. Select environment: `dev`, `qa`, or `prod`
5. Click **Run workflow**

### Workflow Steps

The workflow performs these steps:

1. **Checkout**: Clone the repository
2. **Install Databricks CLI**: Install latest Databricks CLI v2
3. **Validate Bundle**: Validate DAB configuration syntax
4. **Deploy Bundle**: Deploy notebooks and DLT pipelines to target environment

### Example Workflow Run

```bash
# What happens during deployment:
✓ Checkout code
✓ Setup Python 3.x
✓ Install Databricks CLI v2
✓ Validate bundle for 'qa' environment
✓ Deploy bundle to Databricks workspace
  - Upload notebooks to /Workspace/.bundle/deltaliverepo/qa/
  - Create/Update DLT-Pipeline-Configuration-Driven
  - Create/Update DLT-Refresh-SCD-Tables-Views
✓ Deployment complete
```

## Troubleshooting

### Connection Issues

**Problem**: Runner cannot connect to Databricks
```
Error: Failed to connect to workspace
```

**Solutions**:
- Verify runner is in correct VNet or has VPN access
- Check NSG rules allow outbound to Databricks
- Verify private endpoint configuration
- Test DNS resolution: `nslookup <workspace>.azuredatabricks.net`

### Authentication Issues

**Problem**: Invalid credentials
```
Error: HTTP 403 Forbidden
```

**Solutions**:
- Verify `DATABRICKS_HOST` is correct (include `https://`)
- Regenerate token if expired
- Check token has admin permissions
- For Service Principal, verify it's added to workspace

### Bundle Validation Errors

**Problem**: Bundle validation fails
```
Error: Invalid configuration in databricks.yaml
```

**Solutions**:
- Check YAML syntax
- Verify notebook paths exist
- Ensure catalog and schema names are valid
- Review bundle with: `databricks bundle validate -t <env>`

### Deployment Failures

**Problem**: Deployment succeeds but pipelines fail
```
Error: Pipeline creation failed
```

**Solutions**:
- Check catalog exists and user has permissions
- Verify volume paths are accessible
- Ensure cluster configurations are valid
- Review Databricks job run logs

## Security Best Practices

1. **Use Service Principals**: Avoid using personal access tokens in production
2. **Rotate Tokens Regularly**: Set expiration dates on tokens
3. **Least Privilege**: Grant minimum required permissions
4. **Secret Management**: Never commit tokens to Git
5. **Network Isolation**: Keep runner in private network
6. **Audit Logging**: Enable Databricks audit logs
7. **IP Allowlisting**: Configure IP access lists if available

## Advanced Configuration

### Custom Network Routes

If using custom routes or forced tunneling:

```yaml
# In databricks.yaml, you may need to configure:
workspace:
  azure_workspace_resource_id: "/subscriptions/.../resourceGroups/.../providers/Microsoft.Databricks/workspaces/..."
```

### Multiple VNets

If deploying across multiple VNets:

```yaml
targets:
  dev:
    workspace:
      host: https://dev-workspace.azuredatabricks.net  # VNet 1
  prod:
    workspace:
      host: https://prod-workspace.azuredatabricks.net  # VNet 2
```

Ensure runner can reach both VNets via peering or hub-spoke topology.

### Using Azure Private Link

For Azure Private Link setup:

1. Create Private Endpoint for Databricks workspace
2. Configure Private DNS Zone
3. Link Private DNS Zone to runner VNet
4. Update `DATABRICKS_HOST` to use private endpoint FQDN

## Monitoring and Alerting

### GitHub Actions Monitoring

- View workflow runs in Actions tab
- Enable email notifications for failures
- Set up Slack/Teams integration for alerts

### Databricks Monitoring

- Monitor job runs in Databricks UI
- Set up email alerts for pipeline failures
- Use Databricks System Tables for audit logs

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Azure Databricks VNet Injection](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/cloud-configurations/azure/vnet-inject)
- [GitHub Self-Hosted Runners](https://docs.github.com/en/actions/hosting-your-own-runners)
- [Databricks Private Link](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/cloud-configurations/azure/private-link)

## Support

For issues or questions:
1. Check workflow run logs in GitHub Actions
2. Review Databricks job run logs
3. Consult network team for connectivity issues
4. Open an issue in this repository
