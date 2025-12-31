# Quick Setup Guide for VNet-Injected Databricks Deployment

This guide provides a step-by-step setup for deploying to VNet-injected Azure Databricks workspaces using GitHub Actions.

## Prerequisites Checklist

- [ ] Azure Databricks workspace deployed with VNet injection
- [ ] Azure VM or runner environment in the same VNet (or with VPN/ExpressRoute access)
- [ ] Databricks workspace admin access
- [ ] GitHub repository admin access

## Step 1: Set Up Self-Hosted GitHub Runner

### 1.1 Create Azure VM in the Same VNet

```bash
# Variables
RESOURCE_GROUP="<your-resource-group>"
VM_NAME="github-runner-vm"
VNET_NAME="<databricks-vnet-name>"
SUBNET_NAME="<runner-subnet-name>"  # Create a subnet for runners if needed
LOCATION="<your-location>"

# Create VM
az vm create \
  --resource-group $RESOURCE_GROUP \
  --name $VM_NAME \
  --location $LOCATION \
  --vnet-name $VNET_NAME \
  --subnet $SUBNET_NAME \
  --image Ubuntu2204 \
  --size Standard_D2s_v3 \
  --admin-username azureuser \
  --generate-ssh-keys \
  --public-ip-address "" \
  --nsg ""

# Get VM details
az vm show --resource-group $RESOURCE_GROUP --name $VM_NAME
```

### 1.2 Install GitHub Runner on the VM

```bash
# SSH into the VM (use bastion host or VPN)
ssh azureuser@<vm-private-ip>

# Create a folder for the runner
mkdir actions-runner && cd actions-runner

# Download the latest runner package
curl -o actions-runner-linux-x64-2.311.0.tar.gz -L \
  https://github.com/actions/runner/releases/download/v2.311.0/actions-runner-linux-x64-2.311.0.tar.gz

# Extract the installer
tar xzf ./actions-runner-linux-x64-2.311.0.tar.gz

# Configure the runner (use token from GitHub)
# Go to: GitHub Repo → Settings → Actions → Runners → New self-hosted runner
./config.sh --url https://github.com/<your-org>/<your-repo> --token <your-token>

# When prompted for labels, add: databricks-vnet

# Install as a service
sudo ./svc.sh install
sudo ./svc.sh start
```

### 1.3 Install Dependencies on Runner

```bash
# Install required packages
sudo apt-get update
sudo apt-get install -y python3 python3-pip curl jq git

# Verify installations
python3 --version
curl --version
jq --version
```

## Step 2: Configure GitHub Secrets

Go to: **GitHub Repository → Settings → Secrets and variables → Actions**

### 2.1 Add Environment Secrets

For each environment (dev, qa, prod), create the following secrets:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `DATABRICKS_HOST` | Databricks workspace URL | `https://adb-xxxxx.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Databricks access token | `dapi...` |

### 2.2 Generate Databricks Token

```bash
# Option 1: Personal Access Token (for testing)
# In Databricks UI:
# - Click your username (top right)
# - Settings → Developer → Access Tokens
# - Generate New Token → Copy token

# Option 2: Service Principal (recommended for production)
# Create service principal
az ad sp create-for-rbac --name databricks-github-actions

# Add service principal to Databricks workspace
# Databricks UI → Settings → Identity & Access → Service Principals → Add
```

### 2.3 Test Connectivity

```bash
# On your self-hosted runner, test the connection
export DATABRICKS_HOST="https://adb-xxxxx.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."

curl -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  $DATABRICKS_HOST/api/2.0/clusters/list

# Should return JSON with cluster list (or empty list)
```

## Step 3: Configure GitHub Environments

Go to: **GitHub Repository → Settings → Environments**

### 3.1 Create Environments

Create three environments:
- **dev** (Development)
- **qa** (QA/Staging)  
- **prod** (Production)

### 3.2 Configure Environment Protection Rules

For **qa** environment:
- [ ] Add required reviewers (1-2 people)
- [ ] Wait timer: 0 minutes

For **prod** environment:
- [ ] Add required reviewers (2+ people)
- [ ] Wait timer: 5 minutes
- [ ] Restrict deployment to specific branches (e.g., `main`)

### 3.3 Add Environment-Specific Secrets

For each environment, add:
- `DATABRICKS_HOST` - Environment-specific workspace URL
- `DATABRICKS_TOKEN` - Environment-specific token

## Step 4: Update Databricks Bundle Configuration

Edit `asset-bundle/databricks.yaml`:

```yaml
targets:
  dev:
    workspace:
      host: https://your-dev-workspace.azuredatabricks.net
    variables:
      catalog_name: your_dev_catalog
      volume_path: /Volumes/your_dev_catalog/dlt/files
      num_records: "100"
  
  qa:
    workspace:
      host: https://your-qa-workspace.azuredatabricks.net
    variables:
      catalog_name: your_qa_catalog
      volume_path: /Volumes/your_qa_catalog/dlt/files
      num_records: "500"
  
  prod:
    workspace:
      host: https://your-prod-workspace.azuredatabricks.net
    variables:
      catalog_name: your_prod_catalog
      volume_path: /Volumes/your_prod_catalog/dlt/files
      num_records: "1000"
```

## Step 5: Test the Workflow

### 5.1 Manual Test Run

1. Go to **Actions** tab in GitHub
2. Select **Deploy to VNet-Injected Azure Databricks using DAB**
3. Click **Run workflow**
4. Select environment: **dev**
5. Click **Run workflow**

### 5.2 Verify Deployment

```bash
# On your runner or local machine with Databricks CLI
export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."

# List deployed notebooks
databricks workspace list /Workspace/.bundle/deltaliverepo/dev/

# List DLT pipelines
databricks pipelines list-pipelines | jq '.[] | select(.name | contains("DLT"))'
```

### 5.3 Check Pipeline in Databricks UI

1. Login to Databricks workspace
2. Go to **Workflows** → **Delta Live Tables**
3. Find pipelines:
   - `DLT-Pipeline-Configuration-Driven`
   - `DLT-Refresh-SCD-Tables-Views`
4. Verify configuration and try running one

## Step 6: Network Verification

### 6.1 Verify Private Endpoint

```bash
# Check DNS resolution
nslookup your-workspace.azuredatabricks.net

# Should resolve to a private IP address (10.x.x.x or 172.x.x.x or 192.168.x.x)
```

### 6.2 Verify Network Security Groups

```bash
# Check NSG rules allow outbound to Databricks control plane
az network nsg rule list \
  --resource-group $RESOURCE_GROUP \
  --nsg-name <runner-nsg-name> \
  --output table
```

### 6.3 Test Databricks API Access

```bash
# From the runner VM
curl -v https://your-workspace.azuredatabricks.net/api/2.0/clusters/list \
  -H "Authorization: Bearer $DATABRICKS_TOKEN"

# Should return 200 OK
```

## Troubleshooting

### Runner Not Connecting to GitHub

```bash
# Check runner status
sudo ./svc.sh status

# Check runner logs
journalctl -u actions.runner.* -f

# Restart runner service
sudo ./svc.sh stop
sudo ./svc.sh start
```

### Cannot Reach Databricks Workspace

```bash
# Check connectivity
telnet your-workspace.azuredatabricks.net 443

# Check private endpoint configuration
az network private-endpoint list \
  --resource-group $RESOURCE_GROUP \
  --output table
```

### Bundle Validation Fails

```bash
# Test bundle locally on runner
cd /home/azureuser/actions-runner/_work/<repo>/<repo>/asset-bundle
databricks bundle validate -t dev

# Check for syntax errors in databricks.yaml
```

### Pipeline Deployment Fails

```bash
# Check Databricks permissions
databricks workspace whoami

# Verify catalog exists
databricks catalogs list

# Check volume paths
databricks fs ls /Volumes/<catalog>/dlt/
```

## Next Steps

- [ ] Set up monitoring and alerting for workflow failures
- [ ] Configure branch protection rules
- [ ] Set up automated testing before deployment
- [ ] Document runbook for common issues
- [ ] Schedule regular token rotation
- [ ] Set up backup and disaster recovery procedures

## Security Considerations

1. **Token Rotation**: Rotate Databricks tokens every 90 days
2. **Least Privilege**: Use service principal with minimum required permissions
3. **Network Isolation**: Keep runner in private network only
4. **Audit Logging**: Enable Databricks audit logs
5. **Secret Scanning**: Enable GitHub secret scanning
6. **Runner Security**: Keep runner VM patched and updated

## Support

For additional help:
- Review [README-VNET-DEPLOYMENT.md](README-VNET-DEPLOYMENT.md) for detailed documentation
- Check [Databricks documentation](https://docs.databricks.com/dev-tools/bundles/)
- Consult your network/security team for VNet-specific issues
