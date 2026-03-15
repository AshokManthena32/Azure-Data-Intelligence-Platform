/*
Enterprise Networking Infrastructure for Azure Data Platform.

Implements a secure hub-and-spoke topology concept by defining a Virtual Network,
dedicated subnets for specific workloads (e.g., Databricks/Spark, Private Endpoints),
and Network Security Groups (NSGs). This configuration is crucial for ensuring
PaaS services (Storage, KeyVault, Synapse) are not exposed to the public internet.

Author: Lead Cloud Infrastructure Architect
*/

@description('Location for all network resources.')
param location string = resourceGroup().location

@description('The name of the Virtual Network.')
param vnetName string = 'vnet-data-platform-prod'

@description('The address prefix for the Virtual Network.')
param vnetAddressPrefix string = '10.0.0.0/16'

@description('Subnet for Private Endpoints (Storage, KeyVault, etc.).')
param privateEndpointSubnetPrefix string = '10.0.1.0/24'

@description('Subnet for compute workloads (e.g., Self-hosted IR, Spark Clusters).')
param computeSubnetPrefix string = '10.0.2.0/24'


// 1. Network Security Group for Compute Subnet
resource nsgCompute 'Microsoft.Network/networkSecurityGroups@2023-04-01' = {
  name: 'nsg-compute-prod'
  location: location
  properties: {
    securityRules: [
      {
        name: 'DenyAllInbound'
        properties: {
          priority: 4096
          direction: 'Inbound'
          access: 'Deny'
          protocol: '*'
          sourcePortRange: '*'
          destinationPortRange: '*'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: '*'
        }
      }
      // Add specific rules here for allowed traffic, e.g., AzureBastion, internal VNet traffic
    ]
  }
}

// 2. Virtual Network with dedicated Subnets
resource vnet 'Microsoft.Network/virtualNetworks@2023-04-01' = {
  name: vnetName
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetAddressPrefix
      ]
    }
    subnets: [
      {
        name: 'snet-private-endpoints'
        properties: {
          addressPrefix: privateEndpointSubnetPrefix
          // Disable network policies to allow Private Endpoints
          privateEndpointNetworkPolicies: 'Disabled'
          privateLinkServiceNetworkPolicies: 'Enabled'
        }
      }
      {
        name: 'snet-compute'
        properties: {
          addressPrefix: computeSubnetPrefix
          networkSecurityGroup: {
            id: nsgCompute.id
          }
          // Service endpoints if needed before moving entirely to Private Endpoints
          serviceEndpoints: [
            {
              service: 'Microsoft.Storage'
            }
            {
              service: 'Microsoft.KeyVault'
            }
          ]
        }
      }
    ]
  }
}

// 3. Example: Private DNS Zone for Storage (Blob)
resource privateDnsZoneBlob 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: 'privatelink.blob.${environment().suffixes.storage}'
  location: 'global'
}

resource privateDnsZoneLinkBlob 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: privateDnsZoneBlob
  name: '${vnetName}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

// Outputs for integration with main.bicep
output vnetId string = vnet.id
output peSubnetId string = vnet.properties.subnets[0].id
output computeSubnetId string = vnet.properties.subnets[1].id
