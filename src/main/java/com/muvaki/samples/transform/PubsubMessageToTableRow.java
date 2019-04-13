package com.muvaki.samples.transform;

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubsubMessageToTableRow extends
    PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

  private final TupleTag<TableRow> successTupleTag;
  private final TupleTag<TableRow> failTupleTag;
  private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageToTableRow.class);


  public PubsubMessageToTableRow(TupleTag successTupleTag, TupleTag failTupleTag) {
    this.successTupleTag = successTupleTag;
    this.failTupleTag = failTupleTag;
  }

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {

    return input.apply("PubSubMessageToTableRow", ParDo.of(
        new DoFn<PubsubMessage, TableRow>() {

          @ProcessElement
          public void processElement(ProcessContext context) {
            PubsubMessage message = context.element();
            try (InputStream is = new ByteArrayInputStream(message.getPayload())) {
              context.output(successTupleTag, TableRowJsonCoder.of().decode(is, Context.OUTER));
            } catch (IOException e) {
              LOG.error("Could not parse JSON.", e.getCause());

              TableRow tableRow = new TableRow();
              tableRow.set("time_millis", Calendar.getInstance().getTimeInMillis());
              tableRow.set("payload", new String(message.getPayload()));

              context.output(failTupleTag, tableRow);
            }
          }
        }).withOutputTags(this.successTupleTag, TupleTagList.of(this.failTupleTag)));
  }
}
