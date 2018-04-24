package com.djo.beam.example.jobs;

import com.djo.beam.example.utils.TableRowToString;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Maps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InMemoryExample {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryExample.class);

    // Create a Java Collection, in this case a List of Strings.
    static final List<String> LINES = Arrays.asList(
            "To be, or not to be: that is the question: ",
            "Whether 'tis nobler in the mind to suffer ",
            "The slings and arrows of outrageous fortune, ",
            "Or to take arms against a sea of troubles, ");

    static final List<KV<String, String>> emailsList = Arrays.asList(
            KV.of("amy", "amy@example.com"),
            KV.of("carl", "carl@example.com"),
            KV.of("julia", "julia@example.com"),
            KV.of("carl", "carl@email.com"));

    static final List<KV<String, String>> phonesList = Arrays.asList(
            KV.of("amy", "111-222-3333"),
            KV.of("james", "222-333-4444"),
            KV.of("amy", "333-444-5555"),
            KV.of("carl", "444-555-6666"));


   static class LogKV extends SimpleFunction<KV<String,Integer>, KV<String,Integer>> {

        @Override
        public KV apply(KV input) {
            LOG.info("KV = KEYS : " + input.getKey() + " ------ VALUE : " + input.getValue());
            return input;
        }

    }


    static class WordLength extends DoFn<String, Integer> {
        private TupleTag<Integer> testInteger1, testInteger2;

        public WordLength(TupleTag<Integer> testInteger1, TupleTag<Integer> testInteger2) {
            this.testInteger1 = testInteger1;
            this.testInteger2 = testInteger2;
        }
        @ProcessElement
        public void processElement(ProcessContext c) {
            String word = c.element();
            c.output(this.testInteger1, word.length());
            c.output(this.testInteger2, word.replaceAll(" ","").length());
        }
    }

    public static void main(String[] args) {

        final TupleTag<Integer> testInteger = new TupleTag<Integer>();
        final TupleTag<Integer> testInteger2 = new TupleTag<Integer>();
        final TupleTag<String> emailsTag = new TupleTag<>();
        final TupleTag<String> phonesTag = new TupleTag<>();

        TupleTagList ttl = TupleTagList.of(testInteger2);
        // Create the pipeline.
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);

        // Apply Create, passing the list and the coder, to create the PCollection.
        PCollection<String> linesText = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
        PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList));
        PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));


        linesText.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())).via(s -> KV.of(s,s.length())))
                 .apply(MapElements.via(new LogKV()));
        PCollectionTuple test = linesText.apply(ParDo.of(new WordLength(testInteger, testInteger2)).withOutputTags(testInteger,ttl));

        PCollection<Integer> resultInteger = test.get(testInteger);
        PCollection<Integer> resultInteger2 = test.get(testInteger2).setCoder(TextualIntegerCoder.of());
        resultInteger.apply(MapElements.into(TypeDescriptors.integers()).via(l -> {
            LOG.info("Element Size" + l);
            return l;
        }));
        resultInteger2.apply(MapElements.into(TypeDescriptors.integers()).via(l -> {
            LOG.info(" INTEGER 2 - Element Size" + l);
            return l;
        }));


        PCollection<KV<String, CoGbkResult>> results =
                KeyedPCollectionTuple
                        .of(emailsTag, emails)
                        .and(phonesTag, phones)
                        .apply(CoGroupByKey.create());

        PCollection<String> contactLines = results.apply(ParDo.of(
                new DoFn<KV<String, CoGbkResult>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        String name = e.getKey();
                        Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
                        Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
                        String formattedResult = "NAME " + name + " EMAIL : " +  emailsIter.toString() + " PHONE : " + phonesIter.toString();
                        LOG.info("RESULT : " + formattedResult);
                        c.output(formattedResult);
                    }
                }
        ));

        p.run();
    }




}
