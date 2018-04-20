package com.djo.beam.example.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface TestSuiteOption extends PipelineOptions {

    @Description("Path of the file to read from")
    String getInputFile();
    void setInputFile(String value);

}