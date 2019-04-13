package com.muvaki.samples.functions;

import com.google.api.services.bigquery.model.TableRow;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Table.Row;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;

public class ContentItemToTableRowFn extends DoFn<ContentItem, TableRow> {

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {

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
