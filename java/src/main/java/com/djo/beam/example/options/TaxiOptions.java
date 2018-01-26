package com.djo.beam.example.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options to Taxi examples.
 */
public interface TaxiOptions extends PipelineOptions {

    @Description("ProjectId where data source topic lives")
    @Default.String("pubsub-public-data")
    @Validation.Required
    String getSourceProject();
    void setSourceProject(String value);

    @Description("TopicId of source topic")
    @Default.String("taxirides-realtime")
    @Validation.Required
    String getSourceTopic();
    void setSourceTopic(String value);

    @Description("ProjectId where data sink topic lives")
    @Validation.Required
    String getSinkProject();
    void setSinkProject(String value);

    @Description("TopicId of sink topic")
    @Default.String("visualizer")
    @Validation.Required
    String getSinkTopic();
    void setSinkTopic(String value);

    @Description("BigQuery table, format : projectid:dataset.table")
    @Validation.Required
    String getTable();
    void setTable(String value);

}
