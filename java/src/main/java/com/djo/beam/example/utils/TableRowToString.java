package com.djo.beam.example.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TableRowToString extends SimpleFunction<TableRow, String> {

    private static final Logger LOG = LoggerFactory.getLogger(TableRowToString.class);
    private static final ObjectMapper MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    @Override
    public String apply(TableRow input) {
        String value = "";
        try {
            value = MAPPER.writeValueAsString(input);
        } catch (JsonProcessingException e) {
            LOG.error("Error while encoding TableRow : serialization error", e);
        }
        return value;
    }

}
