package com.amazonaws.services.kinesisanalytics.sinks;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * a simple CW metric table sink for emitting app aggregated events for min, max, count
 * This class can be used to log any input or output tables to log4j
 *
 */
public class CWMetricTableSink implements AppendStreamTableSink<Row> {
    private String[] fieldNames;
    private TypeInformation[] fieldTypes;
    private String metricTag;
    private CloudwatchMetricSink cloudwatchMetricSink;


    public CWMetricTableSink(String metricTag) {
        this.metricTag = metricTag;
    }

    private CWMetricTableSink(String metricTag, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.metricTag = metricTag;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.cloudwatchMetricSink = new CloudwatchMetricSink(this.metricTag);

    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {

        /* emit metrics to CW

         */
        dataStream.map(row ->
                Arrays.asList(
                        new CloudwatchMetricTupleModel(
                                /* app name */
                                row.getField(0).toString(),
                                "min",
                                fromStringTimestamp(row.getField(2).toString()),
                                Double.parseDouble(row.getField(3).toString())
                        ),
                        new CloudwatchMetricTupleModel(
                                /* app name */
                                row.getField(0).toString(),
                                "max",
                                fromStringTimestamp(row.getField(2).toString()),
                                Double.parseDouble(row.getField(4).toString())
                        ),
                        new CloudwatchMetricTupleModel(
                                /* app name */
                                row.getField(0).toString(),
                                "count",
                                fromStringTimestamp(row.getField(2).toString()),
                                Double.parseDouble(row.getField(5).toString())
                        )

                )
        ).returns(TypeInformation.of(new TypeHint<List<CloudwatchMetricTupleModel>>(){}))
        .addSink(this.cloudwatchMetricSink);
    }


    static Timestamp fromStringTimestamp(String ts) {
        Timestamp timestamp = null;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
            Date parsedDate = dateFormat.parse(ts);
            timestamp = new Timestamp(parsedDate.getTime());
        } catch (Exception e) {
        }
        return timestamp;
    }


    @Override
    public TypeInformation<Row> getOutputType() {
        return TypeInformation.of(new TypeHint<Row>() {});
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {

        return new CWMetricTableSink(metricTag, fieldNames, fieldTypes);
    }

    @Override
    public String[] getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return this.fieldTypes;
    }
}
