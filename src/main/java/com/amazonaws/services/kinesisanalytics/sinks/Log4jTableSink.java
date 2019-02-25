package com.amazonaws.services.kinesisanalytics.sinks;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * Log4j table sink.
 * This class can be used to log any input or output tables to log4j
 *
 */
public class Log4jTableSink implements AppendStreamTableSink<Row> {
    private String[] fieldNames;
    private TypeInformation[] fieldTypes;
    private Log4jSink<Row> log4jSink;
    private String logHeader;


    public Log4jTableSink(String logHeader) {
        this.logHeader = logHeader;
    }

    private Log4jTableSink(String logHeader, String[] fieldNames, TypeInformation<?>[] fieldTypes, Log4jSink<Row> sink) {
        this.logHeader = logHeader;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.log4jSink = sink;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.addSink(this.log4jSink);
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return TypeInformation.of(new TypeHint<Row>() {});
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        Log4jSink<Row> sink = new Log4jSink<>(logHeader);
        return new Log4jTableSink(logHeader, fieldNames, fieldTypes, sink);
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
