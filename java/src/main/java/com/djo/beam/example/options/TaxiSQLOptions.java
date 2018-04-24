package com.djo.beam.example.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface TaxiSQLOptions extends GcpOptions, PipelineOptions, BigQueryOptions {

    @Description("BigQuery table, format : projectid:dataset.table")
    @Validation.Required
    String getTable();
    void setTable(String value);

}
