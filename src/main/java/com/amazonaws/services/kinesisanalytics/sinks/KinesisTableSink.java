package com.amazonaws.services.kinesisanalytics.sinks;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * Kinesis table sink that can be used with table api to write an output to kinesis stream.
 * NOTE: This implementation only support an already serialized string as the output type.
 *
 */
public class KinesisTableSink implements AppendStreamTableSink<Row> {
    private String[] fieldNames;
    private TypeInformation[] fieldTypes;
    private FlinkKinesisProducer<String> kinesisProducerSink;


    public KinesisTableSink(FlinkKinesisProducer<String> sink) {
        this.kinesisProducerSink = sink;
    }

    private KinesisTableSink(String[] fieldNames,
                            TypeInformation<?>[] fieldTypes,
                            FlinkKinesisProducer<String> sink) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.kinesisProducerSink = sink;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        /*
          convert row to csv string before adding sink to it
          note: default Row toString returns a simple csv formatted string
          the value is not using full csv standard
          if dataset requires escaping use a csv formatter
         */
        dataStream.map(row -> row.toString()).addSink(this.kinesisProducerSink);
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return TypeInformation.of(new TypeHint<Row>() {});
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {

        return new KinesisTableSink(fieldNames, fieldTypes, kinesisProducerSink);
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
