package com.muvaki.samples;

import com.google.api.services.bigquery.model.TableRow;
import com.muvaki.samples.options.DLPOptions;
import com.muvaki.samples.transform.PubsubMessageDLPToTableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

public class PubsubTokenizeBQ {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(DLPOptions.class);
    DLPOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(DLPOptions.class);

    run(options);
  }

  private static PipelineResult run(DLPOptions dlpOptions) {
    Pipeline pipeline = Pipeline.create(dlpOptions);

    final TupleTag<TableRow> successTupleTag = new TupleTag<TableRow>() {
    };
    final TupleTag<TableRow> failTupleTag = new TupleTag<TableRow>() {
    };

    /*
     * Steps:
     *
     *  1) Read Pubsub message.
     *  2) Transform Pubsub message to a BigQuery Table row.
     *      -convert Pubsub message to ContentItem for DLP.
     *      -apply DLP on ContentItem.
     *      -convert ContentItem to BigQuery Table row.
     *  3) Write successful messages.
     *  4) Write failed messages.
     *
     */

    PCollectionTuple outputTuple = pipeline
        .apply("ReadPubsubMessages",
            PubsubIO.readMessages().fromTopic(dlpOptions.getInputTopic()))

        .apply("PubsubMessageToTableRow",
            new PubsubMessageDLPToTableRow(dlpOptions, successTupleTag, failTupleTag));

    outputTuple.get(successTupleTag)
        .apply(BigQueryIO.writeTableRows().withoutValidation().withCreateDisposition(
            CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .to(BigQueryHelpers.parseTableSpec(dlpOptions.getTableSpec().get())));

    outputTuple.get(failTupleTag)
        .apply(BigQueryIO.writeTableRows().withoutValidation().withCreateDisposition(
            CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .to(BigQueryHelpers.parseTableSpec(dlpOptions.getErrorTableSpec().get())));

    return pipeline.run();
  }

}
