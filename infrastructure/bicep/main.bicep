/*
  Infrastructure as Code: Azure Data Intelligence Platform
  Description: Provisions core platform resources for Data & AI.
  Author: Azure Cloud Data Engineer
*/

@description('Location for all resources.')
param location string = resourceGroup().location

@description('The name of the environment (dev, test, prod).')
param environment string = 'prod'

@description('The prefix for all resource names.')
param prefix string = 'adip'

var uniqueSuffix = uniqueString(resourceGroup().id)
var storageAccountName = take('${prefix}st${environment}${uniqueSuffix}', 24)
var synapseWorkspaceName = '${prefix}-synw-${environment}-${uniqueSuffix}'
var amlWorkspaceName = '${prefix}-mlw-${environment}-${uniqueSuffix}'
var keyVaultName = '${prefix}-kv-${environment}-${uniqueSuffix}'
var applicationInsightsName = '${prefix}-ai-${environment}-${uniqueSuffix}'

// 1. Storage Account (ADLS Gen2)
resource storageAccount 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true // ADLS Gen2
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
  }
}

// 2. Storage Containers (Medallion Layers)
resource bronzeContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
  name: '${storageAccount.name}/default/bronze'
}

resource silverContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
  name: '${storageAccount.name}/default/silver'
}

resource goldContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
  name: '${storageAccount.name}/default/gold'
}

// 3. Application Insights
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: applicationInsightsName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
  }
}

// 4. Key Vault
resource keyVault 'Microsoft.KeyVault/vaults@2022-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    accessPolicies: []
    enabledForDeployment: true
    enabledForTemplateDeployment: true
  }
}

// 5. Azure Machine Learning Workspace
resource amlWorkspace 'Microsoft.MachineLearningServices/workspaces@2022-12-01-preview' = {
  name: amlWorkspaceName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    friendlyName: 'Data Intelligence ML Workspace'
    storageAccount: storageAccount.id
    keyVault: keyVault.id
    applicationInsights: appInsights.id
  }
}

// Outputs
output storageAccountName string = storageAccount.name
output amlWorkspaceName string = amlWorkspace.name
output synapseWorkspaceName string = synapseWorkspaceName
