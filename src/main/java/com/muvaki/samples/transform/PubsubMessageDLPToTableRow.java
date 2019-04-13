package com.muvaki.samples.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Table.Row;
import com.google.privacy.dlp.v2.Value;
import com.muvaki.samples.options.DLPOptions;
import com.muvaki.samples.util.Utils;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
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

public class PubsubMessageDLPToTableRow extends
    PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

  private final DLPOptions dlpOptions;
  private final TupleTag<TableRow> successTupleTag;
  private final TupleTag<TableRow> failTupleTag;

  private static final TupleTag<ContentItem> SUCCESS_TRANSFORM_CONTENTITEM_TUPLE_TAG = new TupleTag<ContentItem>() {
  };
  private static final TupleTag<ContentItem> SUCCESS_DLP_TUPLE_TAG = new TupleTag<ContentItem>() {
  };
  private static final TupleTag<TableRow> FAIL_TUPLE_TAG = new TupleTag<TableRow>() {
  };


  private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageDLPToTableRow.class);

  public PubsubMessageDLPToTableRow(DLPOptions dlpOptions, TupleTag<TableRow> successTupleTag,
      TupleTag<TableRow> failTupleTag) {
    this.dlpOptions = dlpOptions;
    this.successTupleTag = successTupleTag;
    this.failTupleTag = failTupleTag;
  }

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {

    List<String> piiFields = Utils.getPIIFields(dlpOptions.getPIIFields().get());

    PCollectionTuple pubSubMessageToContentItemTuple = input
        .apply("PubSubMessageToContentItem", ParDo.of(new PubSubMessageToContentItemFn(piiFields))
            .withOutputTags(SUCCESS_TRANSFORM_CONTENTITEM_TUPLE_TAG,
                TupleTagList.of(FAIL_TUPLE_TAG)));

    PCollectionTuple applyDLPFromTemplateTuple = pubSubMessageToContentItemTuple
        .get(SUCCESS_TRANSFORM_CONTENTITEM_TUPLE_TAG)
        .apply("ApplyDLPFromTemplate", ParDo.of(new ApplyDLPFromTemplateFn(dlpOptions.getProject(),
            dlpOptions.getDLPTemplateName().get())).withOutputTags(
            SUCCESS_DLP_TUPLE_TAG, TupleTagList.of(FAIL_TUPLE_TAG)
        ));

    return PCollectionTuple.of(
        this.successTupleTag, applyDLPFromTemplateTuple
            .get(SUCCESS_DLP_TUPLE_TAG)
            .apply("ContentItemToTableRow", ParDo.of(new ContentItemToTableRowFn()))).and(
        this.failTupleTag, applyDLPFromTemplateTuple.get(FAIL_TUPLE_TAG)
    );

  }

  static class PubSubMessageToContentItemFn extends DoFn<PubsubMessage, ContentItem> {

    private final List<String> piiFields;

    public PubSubMessageToContentItemFn(List<String> piiFields) {
      this.piiFields = piiFields;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();

      JsonParser jsonParser = new JsonParser();

      try {
        JsonObject jsonObject = jsonParser.parse(new String(message.getPayload()))
            .getAsJsonObject();

        Row.Builder rowBuilder = Row.newBuilder();

        List<FieldId> fieldsList = Lists.newArrayList();

        for (Entry<String, JsonElement> entry : jsonObject.entrySet()) {

          String key = entry.getKey();
          String value = entry.getValue().getAsString();

          //Base32 encode PII value to confirm with FPE alphabet.
          if (this.piiFields.stream().anyMatch(field -> field.equals(key))) {
            value = BaseEncoding.base32()
                .encode(value.getBytes());
          }

          fieldsList.add(FieldId.newBuilder().setName(key).build());
          rowBuilder.addValues(Value.newBuilder().setStringValue(value).build());
        }

        Table.newBuilder().addAllHeaders(fieldsList).addRows(rowBuilder.build());

        context.output(SUCCESS_TRANSFORM_CONTENTITEM_TUPLE_TAG, ContentItem.newBuilder()
            .setTable(
                Table.newBuilder()
                    .addAllHeaders(fieldsList)
                    .addRows(rowBuilder.build())
            ).build());
      } catch (RuntimeException e) {
        LOG.error("Could not parse JSON.", e.getCause());

        context.output(FAIL_TUPLE_TAG,
            Utils.getErrorTableRow(message.getPayload(), e.getMessage()));
      }
    }
  }

  static class ContentItemToTableRowFn extends DoFn<ContentItem, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext context) {

      final Table table = context.element().getTable();
      final List<FieldId> headersList = table.getHeadersList();
      final int headersListSize = headersList.size();
      final List<Row> rowsList = table.getRowsList();

      rowsList.forEach(row -> {
        TableRow tableRow = new TableRow();

        for (int i = 0; i < headersListSize; i++) {
          tableRow.set(headersList.get(i).getName(), row.getValues(i).getStringValue());
        }

        context.output(tableRow);
      });
    }
  }

  static class ApplyDLPFromTemplateFn extends DoFn<ContentItem, ContentItem> {

    private final String projectId;
    private final String templateName;

    public ApplyDLPFromTemplateFn(String projectId, String templateName) {
      this.projectId = projectId;
      this.templateName = templateName;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {

      final DeidentifyContentRequest request = DeidentifyContentRequest.newBuilder()
          .setParent(ProjectName.of(this.projectId).toString())
          .setDeidentifyTemplateName(Utils.getFullTemplateName(
              this.projectId,
              this.templateName)
          )
          .setItem(context.element())
          .build();

      try (DlpServiceClient dlpServiceClient = DlpServiceClient.create()) {
        final DeidentifyContentResponse deidentifyContentResponse = dlpServiceClient
            .deidentifyContent(request);

        context.output(SUCCESS_DLP_TUPLE_TAG, deidentifyContentResponse.getItem());
      } catch (IOException e) {
        LOG.error("Could not apply DLP.", e.getCause());

        context.output(FAIL_TUPLE_TAG, Utils.getErrorTableRow(e.getMessage()));
      }
    }
  }
}
