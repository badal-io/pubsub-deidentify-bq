package com.muvaki.samples.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

public interface CustomOptions extends PipelineOptions, DataflowPipelineOptions {

  @Description("Pub/Sub topic to read the input from")
  @Required
  ValueProvider<String> getInputTopic();

  void setInputTopic(ValueProvider<String> value);

  @Description("Dead letter table for message that couldn't be processed. <dataset>.<tablename>")
  @Required
  ValueProvider<String> getErrorTableSpec();

  void setErrorTableSpec(ValueProvider<String> value);

  @Description("BigQuery table spec. <dataset>.<tablename>")
  @Required
  ValueProvider<String> getTableSpec();

  void setTableSpec(ValueProvider<String> value);
}
