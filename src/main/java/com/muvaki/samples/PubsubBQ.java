package com.muvaki.samples;

import com.google.api.services.bigquery.model.TableRow;
import com.muvaki.samples.options.CustomOptions;
import com.muvaki.samples.transform.PubsubMessageToTableRow;
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

public class PubsubBQ {


  public static void main(String[] args) {
    PipelineOptionsFactory.register(CustomOptions.class);
    CustomOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(CustomOptions.class);

    run(options);
  }

  private static PipelineResult run(CustomOptions customOptions) {
    Pipeline pipeline = Pipeline.create(customOptions);

    final TupleTag<TableRow> successTupleTag = new TupleTag<TableRow>() {
    };
    final TupleTag<TableRow> failTupleTag = new TupleTag<TableRow>() {
    };

    /*
     * Steps:
     *
     *  1) Read Pubsub message.
     *  2) Transform Pubsub message to a BigQuery Table row.
     *  3) Write successful messages.
     *  4) Write failed messages.
     *
     */

    PCollectionTuple outputTuple = pipeline
        .apply("ReadPubsubMessages",
            PubsubIO.readMessages().fromTopic(customOptions.getInputTopic()))

        .apply("PubsubMessageToTableRow",
            new PubsubMessageToTableRow(successTupleTag, failTupleTag));

    outputTuple.get(successTupleTag)
        .apply(BigQueryIO.writeTableRows().withoutValidation().withCreateDisposition(
            CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .to(BigQueryHelpers.parseTableSpec(customOptions.getTableSpec().get())));

    outputTuple.get(failTupleTag)
        .apply(BigQueryIO.writeTableRows().withoutValidation().withCreateDisposition(
            CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .to(BigQueryHelpers.parseTableSpec(customOptions.getErrorTableSpec().get())));

    return pipeline.run();
  }
}
