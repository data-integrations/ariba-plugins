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
**API Endpoint (M)**: Ariba Base URL.  
**System Type**: Select system either Prod or Sandbox.  
**Realm (M)**: Realm name of the customer.   
**View Template Name (M)**: Name of the Template.

## OAuth Credentials:

**Ariba Client ID (M)**: Ariba client id.  
**Ariba Client Secret (M)**: Ariba client secret.  
**Ariba API Key (M)**: Ariba application key.

## Advance Option:

**From Date (M, O)**: Date from which the data need to be fetched. e.g. 2021-12-01T00:00:00Z  
**To Date (M, O)**: Date to which the extraction will be done. e.g."2022-03-29T00:00:00Z"


Data Types Mapping
----------

    | Ariba Data Type                | CDAP Schema Data Type | Comment                                            |
    | ------------------------------ | --------------------- | -------------------------------------------------- |
    | number                         | double                |                                                    |
    | boolean                        | boolean               |                                                    |
    | string                         | string                |                                                    |
    | date                           | timestamp_micros      |                                                    |
  
