package com.djo.beam.example.transform;

import com.djo.beam.example.core.RidePoint;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.Combine;

public class LatestPointCombine extends Combine.CombineFn<TableRow, RidePoint, TableRow> {

    public RidePoint createAccumulator() {
        return new RidePoint();
    }

    public RidePoint addInput(RidePoint latest, TableRow input) {
        RidePoint newPoint = new RidePoint(input);
        if (latest.rideId == null || newPoint.timestamp > latest.timestamp) return newPoint;
        else return latest;
    }

    public RidePoint mergeAccumulators(Iterable<RidePoint> latestList) {
        RidePoint merged = createAccumulator();
        for (RidePoint latest : latestList) {
            if (merged.rideId == null || latest.timestamp > merged.timestamp)
                merged = new RidePoint(latest);
        }
        return merged;
    }

    public TableRow extractOutput(RidePoint latest) {
        return latest.toTableRow();
    }
}