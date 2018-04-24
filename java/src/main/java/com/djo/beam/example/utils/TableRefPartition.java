package com.djo.beam.example.utils;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TableRefPartition implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {

    public static final DateTimeFormatter partitionFormatter = DateTimeFormat
            .forPattern("yyyyMMdd")
            .withZoneUTC();

    private final String projectId;
    private final String datasetId;

    private final String partitionPrefix; // e.g. tablename$
    private final String fieldName; // Instant or Number field that defined the time dimension for partitions
    private final Boolean timeField; // if yes, then extract time and do the conversion to number, otherwise just take the value as partition
    private final Boolean window; // if yes, then extract time and do the conversion to number, otherwise just take the value as partition

    private TableRefPartition(String projectId, String datasetId, String partitionPrefix, String fieldName, Boolean timeField, Boolean window) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.partitionPrefix = partitionPrefix;

        this.fieldName = fieldName;
        this.timeField = timeField;
        this.window = window;
    }

    public static TableRefPartition perDay(String projectId, String datasetId, String table) {
        return new TableRefPartition(projectId, datasetId, table + "$", null, false, true);
    }

    public static TableRefPartition perDay(String projectId, String datasetId, String table, String fieldName, Boolean isTimeField) {
        return new TableRefPartition(projectId, datasetId, table + "$", fieldName, isTimeField, false);
    }

    private String prefixValue(ValueInSingleWindow<TableRow> input) {
        if (this.window) return input.getWindow().maxTimestamp().toString(partitionFormatter);

        if (this.timeField) {
            String sTime = (String) input.getValue().get(this.fieldName);
            Instant time = Instant.parse(sTime);
            return time.toString(partitionFormatter);
        } else {
            return ((Integer) input.getValue().get(this.fieldName)).toString();
        }
    }

    /**
     * input - a tupel that contains the data element (TableRow), the window, the timestamp, and the pane
     */
    @Override
    public TableDestination apply(ValueInSingleWindow<TableRow> input) {
        TableReference reference = new TableReference();
        reference.setProjectId(this.projectId);
        reference.setDatasetId(this.datasetId);
        reference.setTableId(this.partitionPrefix + prefixValue(input));
        return new TableDestination(reference, null);
    }
}
