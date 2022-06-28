# SAP Ariba Batch Source
## Description
The Ariba Batch Source plugin enables bulk data integration from Ariba
applications with the Cloud Data Fusion platform. You can configure and execute
bulk data transfers from Ariba without any coding.

## API Rate Limits
Job Submission:
1/second, 2/minute, 8/hour, 40/day

Fetch Schema:
1/second, 10/minute, 100/hour, 500/day 

File Download:
2/second, 20/minute, 200/hour, 1000/day

## Properties
You can configure the following properties for the Ariba.

**Note**: The following indicators are used to define the fields:  
**M** - Indicates Macros are supported for the respective field  
**O** - Optional field

**Reference Name:** Name used to uniquely identify this source for lineage,
annotating metadata, etc.  
**API Endpoint (M)**: Base path of the Ariba API, for example, https://sandbox.api.sap.com.  
**System Type**: Type of system the Ariba instance is running on: Production or Sandbox.  
**Realm (M)**: Realm name from which the data is to be extracted, for example, S4VALL.   
**View Template Name (M)**: Name of the view template from which data is to be extracted, for example, RequestFactSystemView.

## OAuth Credentials:

**Ariba Client ID (M)**: Ariba Client ID.  
**Ariba Client Secret (M)**: Ariba Client Secret.  
**Ariba API Key (M)**: Ariba API Key.

## Advance Option:

**From Date (M, O)**: Start date of the extraction, for example, "2021-12-01T00:00:00Z".  
**To Date (M, O)**: End date of the extraction, for example, "2022-03-29T00:00:00Z".


Data Types Mapping
----------

    | Ariba Data Type                | CDAP Schema Data Type | Comment                                            |
    | ------------------------------ | --------------------- | -------------------------------------------------- |
    | number                         | double                |                                                    |
    | boolean                        | boolean               |                                                    |
    | string                         | string                |                                                    |
    | date                           | timestamp_micros      |                                                    |
    | array                          | array                 |                                                    |
    | object                         | record                |                                                    |

  
