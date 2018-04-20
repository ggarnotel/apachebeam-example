package com.djo.beam.example.jobs;

import com.djo.beam.example.WordCount;
import com.djo.beam.example.options.FileOptions;
import com.djo.beam.example.utils.StringToTableRow;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class JSONFile {

    static final List<String> LINES = Arrays.asList(
            "To be, or not to be: that is the question: ",
            "Whether 'tis nobler in the mind to suffer ",
            "The slings and arrows of outrageous fortune, ",
            "Or to take arms against a sea of troubles, ");


    static final Logger LOG = LoggerFactory.getLogger(JSONFile.class);

    static class LogTableRow extends SimpleFunction<TableRow, TableRow> {

        @Override
        public TableRow apply(TableRow input) {
            LOG.info("TableRow : " + input.toString());
            return input;
        }

    }

    static class PassTableRow extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow input = c.element();
            c.output(input);
        }
    }

    static class TestSide extends DoFn<String, String> {

        private PCollectionView<TableRow> t;

        public TestSide(PCollectionView<TableRow> t) {
            this.t = t;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String input = c.element();
            TableRow tableRow = c.sideInput(t);
            LOG.info("input " + input + " t " +tableRow.toString());
            c.output(input);
        }
    }


    public static void main(String[] args) {
        FileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FileOptions.class);
        Pipeline p = Pipeline.create(options);
        PCollection<TableRow> t = p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
        .apply(MapElements.via(new StringToTableRow()))
        .apply(MapElements.via(new LogTableRow()))
        .apply(ParDo.of(new PassTableRow()));

        PCollectionView<TableRow> test = t.apply(View.asSingleton());

        PCollection<String> linesText = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
        linesText.apply(ParDo.of(new TestSide(test)).withSideInputs(test));

        p.run();
    }

}
