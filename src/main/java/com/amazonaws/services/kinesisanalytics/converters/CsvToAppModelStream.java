package com.amazonaws.services.kinesisanalytics.converters;

import com.amazonaws.services.kinesisanalytics.AppModel;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CsvToAppModelStream {
    private static final int CSV_APP_NAME_COL_INDEX = 0;
    private static final int CSV_TIMESTAMP_COL_INDEX = 1;
    private static final int CSV_APP_ID_COL_INDEX = 2;
    private static final int CSV_VERSION_COL_INDEX = 3;

    private static Logger LOG = LoggerFactory.getLogger(CsvToAppModelStream.class);

    /**
     * convert raw CSV to DataStream<AppModel>.
     * csv input format is assumed as:
     *    appName, timestamp, appId, version
     * @param inputCsvStream input csv stream
     * @return DataStream instance for AppModel
     */
    public static DataStream<AppModel> convert(DataStream<String> inputCsvStream){
        return inputCsvStream.map(csvRow -> {
                    String [] csvFields = csvRow.split(",");

                    Timestamp timestamp = null;
                    try {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                        Date parsedDate = dateFormat.parse(csvFields[CSV_TIMESTAMP_COL_INDEX]);
                        timestamp = new java.sql.Timestamp(parsedDate.getTime());
                    } catch (Exception e) {
                        LOG.error("Error processing timestamp " + e.toString());
                        //in production app have a strategy on dealing with bad input message
                        //e.g. send them to dead-letter queue (err... stream)
                    }
                    int version = 0;
                    try {
                        version = Integer.parseInt(csvFields[CSV_VERSION_COL_INDEX]);
                    } catch (Exception e) {
                        LOG.error("Error processing version attribute " + e.toString());

                        //
                    }

                    return new AppModel(
                            csvFields[CSV_APP_NAME_COL_INDEX],
                            csvFields[CSV_APP_ID_COL_INDEX],
                            version,
                            timestamp
                    );

                }).returns(AppModel.class)
                .name("inputAppModelStream")
                //assign timestamp for time window processing
                .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
                .name("timestamp");

    }
}
