package com.muvaki.samples.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

public interface DLPOptions extends CustomOptions {

  @Description("DLP Template name.  This must exist already.")
  ValueProvider<String> getDLPTemplateName();

  void setDLPTemplateName(ValueProvider<String> value);

  @Description("Comma separated string of columns to tokenize.  Must match template.")
  @Required
  ValueProvider<String> getPIIFields();

  void setPIIFields(ValueProvider<String> value);
}
