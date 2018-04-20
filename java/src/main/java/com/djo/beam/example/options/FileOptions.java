package com.djo.beam.example.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface FileOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of
     * King Lear. Set this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();
    void setInputFile(String value);

}
