package com.muvaki.samples.util;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {

  public static final String DEFAULT_PII_SEPARATOR = ",";

  public static List<String> getPIIFields(String delimitedSeparatedList) {
    return Arrays.asList(delimitedSeparatedList.split(DEFAULT_PII_SEPARATOR)).stream()
        .collect(Collectors.toList());
  }

  public static List<String> getPIIFields(String delimitedSeparatedList, String delimiter) {
    return Arrays.asList(delimitedSeparatedList.split(delimiter)).stream()
        .collect(Collectors.toList());
  }

  public static String getFullTemplateName(String projectName, String templateId) {
    return String.format("projects/%s/deidentifyTemplates/%s",
        projectName,
        templateId
    );
  }

  public static TableRow getErrorTableRow(byte[] payload, String errorMsg) {

    TableRow tableRow = new TableRow();
    tableRow.set("time_millis", Calendar.getInstance().getTimeInMillis());
    tableRow.set("payload", new String(payload));
    tableRow.set("error_msg", errorMsg);

    return tableRow;
  }

  public static TableRow getErrorTableRow(String errorMsg) {

    TableRow tableRow = new TableRow();
    tableRow.set("time_millis", Calendar.getInstance().getTimeInMillis());
    tableRow.set("error_msg", errorMsg);

    return tableRow;
  }

}
