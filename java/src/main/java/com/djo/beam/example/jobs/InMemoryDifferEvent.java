package com.djo.beam.example.jobs;

import com.djo.beam.example.utils.StringToTableRow;
import com.djo.beam.example.utils.TableRowToKV;
import com.djo.beam.example.utils.TableRowToString;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.TableView;
import java.util.Arrays;
import java.util.List;

public class InMemoryDifferEvent {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryDifferEvent.class);

    static final List<String> LINES = Arrays.asList(
//            "{\"id\":\"1\",\"search\":\"search1\",\"name\":\"nameSearch1\",\"timestamp\":1520371945}",
            "{\"id\":\"2\",\"search\":\"search2\",\"name\":\"nameSearch2\",\"timestamp\":1520372945}",
            "{\"id\":\"2\",\"id2\":\"A4\",\"resultsearch\":\"resultsearch1\",\"name\":\"nameResultSearch1\",\"timestamp\":1520373845 }",
            "{\"id\":\"2\",\"id2\":\"A4\",\"resultsearch\":\"resultsearch2\",\"name\":\"nameResultSearch2\",\"timestamp\":1520373942 }",
            "{\"id\":\"2\",\"id2\":\"A4\",\"resultsearch\":\"resultsearch3\",\"name\":\"nameResultSearch3\",\"timestamp\":1520373945 }",
            "{\"id\":\"2\",\"id2\":\"A4\",\"resultsearch\":\"resultsearch4\",\"name\":\"nameResultSearch4\",\"timestamp\":1520374005 }",
            "{\"id2\":\"A4\",\"choice\":\"choice1\",\"name\":\"choice1\" }",
            "{\"id2\":\"A4\",\"choice\":\"choice2\",\"name\":\"choice2\" }");


    static class SeparateEvent extends DoFn<TableRow, TableRow> {
        private TupleTag<TableRow> eventSearch, eventResultSearch,eventChoise;

        public SeparateEvent(TupleTag<TableRow> eventSearch, TupleTag<TableRow> eventResultSearch, TupleTag<TableRow> eventChoise) {
            this.eventSearch = eventSearch;
            this.eventResultSearch = eventResultSearch;
            this.eventChoise = eventChoise;

        }
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();

            if (row.containsKey("id") && row.containsKey("id2"))
                c.output(this.eventResultSearch, row);
            else {
                if (row.containsKey("id"))
                    c.output(this.eventSearch, row);
                else
                    c.output(this.eventChoise, row);
            }
        }

    }

    static class LogTableRow extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            LOG.info(row.toString());
            c.output(row);
        }
    }

    public static void main(String[] args) {
        final TupleTag<TableRow> eventSearch = new TupleTag<>();
        final TupleTag<TableRow> eventResultSearch = new TupleTag<>();
        final TupleTag<TableRow> eventChoise = new TupleTag<>();
        final TupleTag<TableRow> searchTag = new TupleTag<>();
        final TupleTag<TableRow> result1Tag = new TupleTag<>();
        final TupleTag<TableRow> result2Tag = new TupleTag<>();
        final TupleTag<TableRow> choiceTag = new TupleTag<>();
        final TupleTag<TableRow> choiceIdTag = new TupleTag<>();

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);

        PCollection<TableRow> linesText = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
                                           .apply(MapElements.via(new StringToTableRow()));

        PCollectionTuple test = linesText.apply(ParDo.of(new SeparateEvent(eventSearch, eventResultSearch, eventChoise)).withOutputTags(eventSearch,TupleTagList.of(eventResultSearch).and(eventChoise)));
        PCollection<TableRow> resultSearch = test.get(eventSearch);
        PCollection<TableRow> resultResultSearch = test.get(eventResultSearch).setCoder(TableRowJsonCoder.of());
        PCollection<TableRow> resultChoise = test.get(eventChoise).setCoder(TableRowJsonCoder.of());


        PCollection<KV<String, TableRow>> result_id2KV = resultResultSearch.apply(MapElements.via(new TableRowToKV("id2")));
        PCollection<KV<String, TableRow>> choiceKV = resultChoise.apply(MapElements.via(new TableRowToKV("id2")));

        PCollection<KV<String, CoGbkResult>> result_choice =
                KeyedPCollectionTuple
                        .of(result2Tag, result_id2KV)
                        .and(choiceTag, choiceKV)
                        .apply(CoGroupByKey.create());

        PCollection<KV<String, TableRow>> choiceRowWithId = result_choice.apply(ParDo.of(
                new DoFn<KV<String, CoGbkResult>, KV<String, TableRow>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        String name = e.getKey();
                        Iterable<TableRow> resultsearch = e.getValue().getAll(result2Tag);
                        TableRow rsRow = resultsearch.iterator().next();
                        String id = (String) rsRow.get("id");
                        LOG.info(rsRow.toString());
                        Iterable<TableRow> result1 = e.getValue().getAll(choiceTag);
                        for (TableRow row : result1) {
                            c.output(KV.of(id, row));
                        }
                    }
                }
        ));



        PCollection<KV<String, TableRow>> searchKV = resultSearch.apply(MapElements.via(new TableRowToKV("id")));
        PCollection<KV<String, TableRow>> result_idKV = resultResultSearch.apply(MapElements.via(new TableRowToKV("id")));

        PCollection<KV<String, CoGbkResult>> results =
                KeyedPCollectionTuple
                        .of(searchTag, searchKV)
                        .and(result1Tag, result_idKV)
                        .and(choiceIdTag, choiceRowWithId)
                        .apply(CoGroupByKey.create());

        PCollection<KV<String, TableRow>> contactLines = results.apply(ParDo.of(
                new DoFn<KV<String, CoGbkResult>, KV<String, TableRow>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        String name = e.getKey();
                        Iterable<TableRow> search = e.getValue().getAll(searchTag);
                        Iterable<TableRow> result1 = e.getValue().getAll(result1Tag);
                        Iterable<TableRow> choice = e.getValue().getAll(choiceIdTag);
                        LOG.info(search.toString() + " RESULT " + result1.toString() + " CHOICE " + choice);
                    }
                }
        ));


        p.run();
    }

}
