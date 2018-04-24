package com.djo.beam.example.jobs;


import com.djo.beam.example.options.TaxiRidesOptions;
import com.djo.beam.example.transform.LatestPointCombine;
import com.djo.beam.example.utils.PubSubToTableRow;
import com.djo.beam.example.utils.TableRefPartition;
import com.djo.beam.example.utils.TableRowToString;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 *
 * ride format from PubSub
 * {
 * "ride_id":"a60ba4d8-1501-4b5b-93ee-b7864304d0e0",
 * "latitude":40.66684000000033,
 * "longitude":-73.83933000000202,
 * "timestamp":"2016-08-31T11:04:02.025396463-04:00",
 * "meter_reading":14.270274,
 * "meter_increment":0.019336415,
 * "ride_status":"enroute",
 * "passenger_count":2
 * }
 *
 */
public class RidesWithDollar {

    private static final Logger LOG = LoggerFactory.getLogger(RidesWithDollar.class);
    private static final TableSchema SCHEMA = new TableSchema().setFields(new ImmutableList.Builder<TableFieldSchema>()
        .add(new TableFieldSchema().setName("ride_id").setType("STRING"))
        .add(new TableFieldSchema().setName("latitude").setType("FLOAT"))
        .add(new TableFieldSchema().setName("longitude").setType("FLOAT"))
        .add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"))
        .add(new TableFieldSchema().setName("meter_reading").setType("FLOAT"))
        .add(new TableFieldSchema().setName("meter_increment").setType("FLOAT"))
        .add(new TableFieldSchema().setName("passenger_count").setType("INTEGER"))
        .add(new TableFieldSchema().setName("ride_status").setType("STRING"))
        .build());

    private static class TransformRides extends DoFn<Double, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c, IntervalWindow window) {
            Double dollars = c.element();
            TableRow r = new TableRow();
            r.set("dollar_turnover", dollars);
            r.set("dollar_timing", c.pane().getTiming()); // EARLY, ON_TIME or LATE

            // the timing can be:
            // EARLY: the dollar amount is not yet final
            // ON_TIME: dataflow thinks the dollar amount is final but late data are still possible
            // LATE: late data has arrived
            r.set("dollar_window", window.start().getMillis() / 1000.0 / 60.0); // timestamp in fractional minutes
            LOG.info("Outputting $ value {}} at {} with marker {} for window {}", dollars.toString(), new Date().getTime(), c.pane().getTiming().toString(), window.hashCode());
            c.output(r);
        }
    }

    public RidesWithDollar() {
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        TaxiRidesOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TaxiRidesOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollection<PubsubMessage> source = p.apply("read from PubSub",PubsubIO.readMessages()
                                        .fromTopic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
                                        .withTimestampAttribute("ts"));

        PCollection<TableRow> event = source.apply(MapElements.via(new PubSubToTableRow()));

        //Exact Dollar
        event.apply("extract dollars",MapElements.into(TypeDescriptors.doubles())
                                                        .via((TableRow x) -> Double.parseDouble(x.get("meter_increment").toString())))
            .apply("fixed window", Window.<Double>into(FixedWindows.of(Duration.standardMinutes(1)))
                                                            .triggering(AfterWatermark.pastEndOfWindow()
                                                                    .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)))
                                                                    .withLateFirings(AfterPane.elementCountAtLeast(1)))
                                                            .accumulatingFiredPanes()
                                                            .withAllowedLateness(Duration.standardMinutes(5)))
            .apply("sum whole window", Sum.doublesGlobally().withoutDefaults())
            .apply("format rides", ParDo.of(new TransformRides()))
            .apply(MapElements.via(new TableRowToString()))
            .apply(PubsubIO.writeStrings().to(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic())));


        //Latest Rides
        PCollection<TableRow> mapPosition = event.apply("key rides by rideid", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),TypeDescriptor.of(TableRow.class)))
                        .via(ride -> KV.of(ride.get("ride_id").toString(), ride)))

            .apply("session windows on rides with early firings", Window.<KV<String, TableRow>>into(Sessions.withGapDuration(Duration.standardMinutes(60)))
                .triggering(AfterWatermark.pastEndOfWindow()
                                          .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(2000))))
                                          .accumulatingFiredPanes()
                                          .withAllowedLateness(Duration.ZERO))
            .apply("group ride points on same ride", Combine.perKey(new LatestPointCombine()))
            .apply("discard key", MapElements.into(TypeDescriptor.of(TableRow.class)).via(a -> a.getValue()));

        mapPosition.apply(MapElements.via(new TableRowToString()))
            .apply(PubsubIO.writeStrings().to(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic())));

        PCollection<TableRow> readyToBQ =  mapPosition.apply("Filter ride status",Filter.by(t -> t.containsKey("ride_status")));

        readyToBQ.apply("Dollar to BigQuery",BigQueryIO.writeTableRows()
                   .to(options.getTable())
                   .withSchema(SCHEMA)
                   .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withTimePartitioning(new TimePartitioning().setField("timestamp")));

/**        readyToBQ.apply("Dollar to BigQuery",BigQueryIO.writeTableRows()
                .to(TableRefPartition.perDay(options.getSinkProject(),options.getDataset(),options.getTable_partition()))
                .withSchema(SCHEMA)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
**/

        p.run();
    }
}
