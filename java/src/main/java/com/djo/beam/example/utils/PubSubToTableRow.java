package com.djo.beam.example.utils;

import com.djo.beam.example.jobs.RidesWithDollar;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class PubSubToTableRow extends SimpleFunction<PubsubMessage, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToTableRow.class);

    @Override
    public TableRow apply(PubsubMessage input) {
        TableRow tableRow = null;
        try {
            String str = new String(input.getPayload(), StandardCharsets.UTF_8);
            tableRow = CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), input.getPayload());
        } catch (CoderException e) {
            LOG.error("Error while decoding Event from pusbSub message: serialization error");
        }
        return tableRow;
    }
}
