package com.muvaki.samples.functions;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Table.Row;
import com.google.privacy.dlp.v2.Value;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

public class PubSubMessageToContentItemFn extends DoFn<PubsubMessage, ContentItem> {

  private final List<String> piiFields;

  public PubSubMessageToContentItemFn(List<String> piiFields) {
    this.piiFields = piiFields;
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    PubsubMessage message = context.element();

    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = jsonParser.parse(new String(message.getPayload())).getAsJsonObject();

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

    context.output(ContentItem.newBuilder()
        .setTable(
            Table.newBuilder()
                .addAllHeaders(fieldsList)
                .addRows(rowBuilder.build())
        ).build());
  }

}
