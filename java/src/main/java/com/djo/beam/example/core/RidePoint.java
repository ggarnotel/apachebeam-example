package com.djo.beam.example.core;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

@DefaultCoder(AvroCoder.class)
public class RidePoint {

    public RidePoint() {}

    public RidePoint(String key) {
        rideId = key;
    }

    public RidePoint(RidePoint p) {
        rideId = p.rideId;
        timestamp = p.timestamp;
        lat = p.lat;
        lon = p.lon;
        status = p.status;
    }

    public RidePoint(TableRow r) {
        lat = Float.parseFloat(r.get("latitude").toString());
        lon = Float.parseFloat(r.get("longitude").toString());
        rideId = r.get("ride_id").toString();
        status = r.get("ride_status").toString();
        timestamp =
                Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(r.get("timestamp").toString()))
                        .toEpochMilli();
    }

    public TableRow toTableRow() {
        TableRow result = new TableRow();
        result.set("latitude", lat);
        result.set("longitude", lon);
        result.set("ride_id", rideId);
        result.set("timestamp", Instant.ofEpochMilli(timestamp).toString());
        result.set("ride_status", status);
        return result;
    }

    public String rideId;
    public long timestamp;
    public float lat;
    public float lon;
    public String status;

}
