package com.djo.beam.example.utils;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class TableRowToKV extends SimpleFunction<TableRow, KV<String, TableRow>> {

    String key;
    public TableRowToKV(String key) {
        this.key = key;
    }

    @Override
    public KV<String, TableRow> apply(TableRow input) {
        return KV.of((String) input.get(key), input);
    }

}

