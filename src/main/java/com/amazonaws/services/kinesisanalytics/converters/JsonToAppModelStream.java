package com.amazonaws.services.kinesisanalytics.converters;

import com.amazonaws.services.kinesisanalytics.AppModel;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;


public class JsonToAppModelStream {

    final static GsonBuilder builder = new GsonBuilder();
    final static Gson gson = builder.setDateFormat("yyyy-MM-dd hh:mm:ss.SSS").create();

    private static Logger LOG = LoggerFactory.getLogger(JsonToAppModelStream.class);

    /**
     * convert JSON data to DataStream<AppModel>.
     *
     * @param inputJsonStream input json stream
     * @return DataStream instance for AppModel
     */
    public static DataStream<AppModel> convert(DataStream<String> inputJsonStream) {
        return inputJsonStream.map(json -> gson.fromJson(json, AppModel.class)
                .withProcessingTime(Timestamp.from(Instant.now(Clock.systemUTC())))
        ).returns(AppModel.class)
                .name("inputAppModelStream")
                //assign timestamp for time window processing
                .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
                .name("processingTimestamp");

    }
}
