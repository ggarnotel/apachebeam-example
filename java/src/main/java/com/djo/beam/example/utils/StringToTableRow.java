package com.djo.beam.example.utils;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class StringToTableRow extends SimpleFunction<String, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(StringToTableRow.class);

    @Override
    public TableRow apply(String input) {
        TableRow tableRow = null;
        try {
            tableRow = CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), input.getBytes());
        } catch (CoderException e) {
            LOG.error("Error while decoding Event from pusbSub message: serialization error");
        }
        return tableRow;
    }
}
