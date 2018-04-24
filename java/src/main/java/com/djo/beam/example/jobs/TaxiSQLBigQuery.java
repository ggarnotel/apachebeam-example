package com.djo.beam.example.jobs;

import com.djo.beam.example.options.TaxiRidesOptions;
import com.djo.beam.example.options.TaxiSQLOptions;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.pubsub.Pubsub;
import com.google.cloud.ServiceOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaxiSQLBigQuery {

    private static final Logger LOG = LoggerFactory.getLogger(TaxiSQLBigQuery.class);


    // use the default project id
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

    /** Publish messages to a topic.
     * @param args topic name, number of messages
     */
/*
    public static void publish(String... args) throws Exception {
        // topic id, eg. "my-topic"
        String topicId = args[0];
        int messageCount = Integer.parseInt(args[1]);
        TopicName topicName = TopicName.of(PROJECT_ID, topicId);
        Publisher publisher = null;
        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            for (int i = 0; i < messageCount; i++) {
                String message = "message-" + i;

                // convert message to bytes
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                        .setData(data)
                        .build();

                //schedule a message to be published, messages are automatically batched
                ApiFuture<String> future = publisher.publish(pubsubMessage);

                // add an asynchronous callback to handle success / failure
                ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

                    @Override
                    public void onFailure(Throwable throwable) {
                        if (throwable instanceof ApiException) {
                            ApiException apiException = ((ApiException) throwable);
                            // details on the API exception
                            System.out.println(apiException.getStatusCode().getCode());
                            System.out.println(apiException.isRetryable());
                        }
                        System.out.println("Error publishing message : " + message);
                    }

                    @Override
                    public void onSuccess(String messageId) {
                        // Once published, returns server-assigned message ids (unique within the topic)
                        System.out.println(messageId);
                    }
                });
            }
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
            }
        }
    }
*/

    /**
     * @param args
     */
    public static void main(String[] args) {
        TaxiSQLOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TaxiSQLOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply("Read data on bigquery", BigQueryIO.readTableRows().from(options.getTable()))
         .apply("test", ParDo.of(new DoFn<TableRow, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                LOG.info(c.element().toString());
                c.output(c.element());
            }
        }));

        p.run().waitUntilFinish();

        LOG.info("******************************************************************");
        LOG.info("pipeline info : " + p.toString());
        LOG.info("******************************************************************");
    }
}

