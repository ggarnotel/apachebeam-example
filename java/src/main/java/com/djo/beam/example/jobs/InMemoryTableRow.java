package com.djo.beam.example.jobs;

import com.djo.beam.example.utils.StringToTableRow;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class InMemoryTableRow {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryExample.class);

    // Create a Java Collection, in this case a List of Strings.
    static final List<String> LINES = Arrays.asList(
            "{\"info\":{\"formatVersion\":\"3.7.0\"},\"monitors\":[{\"type\":\"TPGV\",\"evId\":1666,\"tStampMs\":1521047296587,\"dlg\":\"441.000000\",\"dlv\":\"356.000000\",\"ulg\":\"441.000000\",\"ulv\":\"372.000000\"},{\"type\":\"TPGV\",\"evId\":1667,\"tStampMs\":1521047297597,\"dlg\":\"1213.000000\",\"dlv\":\"1161.000000\",\"ulg\":\"784.000000\",\"ulv\":\"784.000000\"}]}");

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);

        PCollection<TableRow> linesText = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply(MapElements.via(new StringToTableRow()));

        p.run();

    }
}
