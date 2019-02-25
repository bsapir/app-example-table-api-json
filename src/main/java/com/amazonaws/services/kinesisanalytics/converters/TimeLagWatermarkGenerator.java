package com.amazonaws.services.kinesisanalytics.converters;

import com.amazonaws.services.kinesisanalytics.AppModel;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

// for generating timestamp and watermark, required for using any time Window processing
public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<AppModel> {

    private final long maxTimeLag = 5000; // 5 seconds

    @Override
    public long extractTimestamp(AppModel app, long previousElementTimestamp) {
        return app.getTimestamp().toInstant().toEpochMilli();
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current time minus the maximum time lag
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }
}
