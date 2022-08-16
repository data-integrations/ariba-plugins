# SAP Ariba Batch Source
## Description

The Ariba Batch Source plugin lets you integrate bulk data from SAP Ariba
applications with Cloud Data Fusion. You can configure and execute
bulk data transfers from Ariba without any coding.

For more information, see [Connect to SAP Ariba](https://cloud.google.com/data-fusion/docs/how-to/connect-to-sap-ariba-batch-source).

## Properties
You can configure the following properties for the Ariba.

**Note**: The following indicators are used to define the fields:  
**M** - Indicates Macros are supported for the respective field  
**O** - Optional field

## Basic 

**Reference Name:** Name used to uniquely identify this source for lineage,
annotating metadata, etc.  
**API Endpoint (M)**: Base path of the Ariba API, for example, https://sandbox.api.sap.com.  
**System Type**: Type of system the Ariba instance is running on: Production or Sandbox.  
**Realm (M)**: Realm name from which the data is to be extracted, for example, S4VALL.   
**View Template Name (M)**: Name of the view template from which data is to be extracted, for example, RequestFactSystemView.

## Credentials:

**Ariba Client ID (M)**: Ariba Client ID.  
**Ariba Client Secret (M)**: Ariba Client Secret.  
**Ariba API Key (M)**: Ariba API Key.

## Advanced:

**From Date (M, O)**: Start date of the extraction, for example, "2021-12-01T00:00:00Z".  
**To Date (M, O)**: End date of the extraction, for example, "2022-03-29T00:00:00Z".


Data Types Mapping
----------

| Ariba Data Type                | CDAP Schema Data Type |
| ------------------------------ | --------------------- |
| number                         | double                |
| boolean                        | boolean               |
| string                         | string                |
| date                           | timestamp_micros      |
| array                          | array                 |
| object                         | record                |


## Limits

For more information about the SAP Ariba Analytical Reporting API limits, see the
[SAP documentation](https://blogs.sap.com/2021/04/13/sap-ariba-analytical-reporting-api-part-2-sap-ariba-reporting-api-structure/).
The following rate limits impact your Cloud Data Fusion pipelines.

### Time span limits

The number of transactions posted within a certain time period must not exceed the limit.

The time span limit between `From Date` and `To Date` is
one&nbsp;year. Exceeding the limit results in a validation error. For example,
entering the following filter results in a validation error because the time
span is greater than one year: `TimeCreated >= 2021-12-01T00:00:00Z and
TimeCreated <= 2022-03-29T00:00:00Z`.

### API rate limits

**Job submission limits**: 1 per second, 2 per minute, 8 per hour, 40 per day.

When the limits are exceeded during a job, the pipeline fails.

The data extraction limit for one&nbsp;day is two&nbsp;million&nbsp;records. For
example: 40&nbsp;API calls per day * 50,000&nbsp;records extracted per API
call = 2,000,000 (2 million) records per day.

Rate limits are restored regularly at 00:00 AM in the timezone of the SAP Ariba
data center.

### Volume limits

*  The limit is 50,000 records per call. For more information, see the [SAP documentation about limits](https://blogs.sap.com/2021/04/13/sap-ariba-analytical-reporting-api-part-2-sap-ariba-reporting-api-structure/).


  
