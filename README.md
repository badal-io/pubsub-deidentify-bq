# Data Tokenization Examples

There are various ways to tokenize/secure data depending on the use case.  These examples will all read from a Pub/Sub topic that have a JSON structured payload as an input and a BigQuery table as an output.  The following examples will be covered:

1) Consume Pub/Sub message and output to a BigQuery table with the PII fields in plain text.  We will then secure a BigQuery view using IAM permissions which would exclude the PII fields.
2) Consume Pub/Sub message and create a DLP request to apply a one way CryptoHash to the PII fields.  Once tokenized, the rows will be output to BigQuery table.
3) Consume Pub/Sub message and create a DLP request to apply a format preserving tokenization to the PII fields.  Once tokenized, the rows will be output to a BigQuery table.  Because we are leveraging the format perserving tokenization of DLP, we are able to re-identify the PII fields.

I suggest going through the offical DLP documentation to get a better feel of the technology: https://cloud.google.com/dlp/docs/

## Tech Requirements

- Java 8+
- Gradle

## Pre-requisites

Before starting, the following tasks needs to be done for the dataflow pipelines to run seamlessly:

- a Pub/Sub topic must be created.
- a BigQuery dataset and table must be created for the outputted data.
- a BigQuery dataset and table must be created to capture any errors during the Dataflow pipeline.
- If you decide to use a view to hide the PII fields, a different dataset is required for the view since IAM permissions can only be applied at a dataset level.
- Create a staging location in Google Cloud Storage for the Dataflow pipeline.
- The DLP template must exist prior to creating the dataflow job.  CryptoHash/FPE template examples are provided here: https://github.com/muvaki/dlp-deidentify-poc 

## Big Query Table Schemas

The source data table can have whatever fields you want, but this sample will only parse Strings.  The JSON in the Pub/Sub message must conform to the schema in the BigQuery table.

The error table must look like this:

| Field Name | Type |
| ------ | ------ |
| time_millis | NUMERIC |
| payload | STRING |
| error_msg | STRING |

## IAM Permissions

Setup permissions IAM, dataflow SA, etc.

## Getting Started

Clone the repo locally.

### Pub/Sub to BigQuery with Views

Once you have created the dataset and tables, you will have to create an authorized view that excludes the PII fields: https://cloud.google.com/bigquery/docs/share-access-views

When ready, run the following in the parent directory of the repo, replacing any of the params with the <> parentheses:

```sh
$ gradle run -PmainClass=com.muvaki.samples.PubsubBQ -Pargs="--runner=DataflowRunner \
--project=<project_id> \
--stagingLocation=gs://<staging_location> \
--inputTopic=projects/<topic_project_id>/topics/<topic_name> \
--tableSpec=<dataset_name>.<table_name> \
--errorTableSpec=<dataset_name>.<table_name> \
--jobName=<job_name>"
```

### Pub/Sub To DLP CryptoHash To BigQuery

Make sure the CryptoHash DLP template exists before running this and that the PII fields match those in the CryptoHash DLP template.

```sh
$ gradle run -PmainClass=com.muvaki.samples.PubsubTokenizeBQ -Pargs="--runner=DataflowRunner \
--project=<project_id> \
--stagingLocation=gs://<staging_location> \
--inputTopic=projects/<topic_project_id>/topics/<topic_name> \
--tableSpec=<dataset_name>.<table_name> \
--errorTableSpec=<dataset_name>.<table_name> \
--DLPTemplateName=<dlp_template_name> \
--PIIFields=<comma_separated_pii_fields> \
--jobName=<job_name>"
```

### Pub/Sub To DLP FPE To BigQuery

Make sure the FPE DLP template exists before running this and that the PII fields match those in the FPE DLP template.

```sh
$ gradle run -PmainClass=com.muvaki.samples.PubsubTokenizeBQ -Pargs="--runner=DataflowRunner \
--project=<project_id> \
--stagingLocation=gs://<staging_location> \
--inputTopic=projects/<topic_project_id>/topics/<topic_name> \
--tableSpec=<dataset_name>.<table_name> \
--errorTableSpec=<dataset_name>.<table_name> \
--DLPTemplateName=<dlp_template_name> \
--PIIFields=<comma_separated_pii_fields> \
--jobName=<job_name>"
```

## Testing

To do a basic test, publish a message to the created topic using the GCP console:
https://console.cloud.google.com/cloudpubsub/topicList

An example message payload would be:

```json
{"id":"B5B71134-E326-BE27-C76C-1D33EDA14E4A","email":"demos@muvaki.com","username":"demos"}
```

In this test we want to de-identify the email.  If you ran all the jobs above, they should each process the Pub/Sub message and output them to the correct BigQuery table.  All these pipelines only accept one JSON object per Pub/Sub message.  If there are any errors in the payload, they will be captured and outputted to the error table. (errorTableSpec)

## Conclusion

To avoid incurring extra unwanted charges, spin down the dataflow jobs that were created in the above steps.
