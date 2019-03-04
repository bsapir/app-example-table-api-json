/*
 * Flink App Example
 * The app reads stream of app events and check min max and count of events reported
 *
 */

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.converters.JsonToAppModelStream;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.kinesisanalytics.sinks.CWMetricTableSink;
import com.amazonaws.services.kinesisanalytics.sinks.KinesisTableSink;
import com.amazonaws.services.kinesisanalytics.sinks.Log4jTableSink;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.streaming.api.TimeCharacteristic;

/**
 * A sample flink stream processing job.
 *
 * The app reads stream of app events and calculate min, max, and count on version reported
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {


    private static Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    private static String VERSION = "1.1.0";
    private static String DEFAULT_REGION = "us-east-1";
    private static int DEFAULT_PARALLELISM = 4;

    private static Properties appProperties = null;

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        LOG.warn("Starting Kinesis Analytics App Example > Version " + VERSION);

        appProperties = initRuntimeConfigProperties();

        // get input stream name from App properties
        String inputStreamName = getAppProperty("inputStreamName", "");

        if (StringUtils.isBlank(inputStreamName)) {
            LOG.error("inputStreamName should be pass using AppProperties config within create-application API call");
            throw new Exception("inputStreamName should be pass using AppProperties config within create-application API call, aborting ...");
        }

        // get output stream name from App properties
        String outputStreamName = getAppProperty("outputStreamName", "");

        if (StringUtils.isBlank(outputStreamName)) {
            LOG.error("outputStreamName should be pass using AppProperties config within create-application API call");
            throw new Exception("outputStreamName should be pass using AppProperties config within create-application API call, aborting ...");
        }

        // use a specific input stream name
        String region = getAppProperty("region", DEFAULT_REGION);

        LOG.warn("Starting Kinesis Analytics App Sample using " +
                        "inputStreamName {} outputStreamName {} region {} parallelism {}",
                        inputStreamName, outputStreamName, region, env.getParallelism());

        String metricTag = getAppProperty("metricTag", "NullMetricTag");

        // use event time for Time windows
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Add kinesis source
        // Notes: input data stream is a json formatted string
        DataStream<String> inputStream = getInputDataStream(env, inputStreamName, region);

        // Add kinesis output
        //FlinkKinesisProducer<String> kinesisOutputSink = getKinesisOutputSink(outputStreamName, region);
        
        // Add kinesis output1
        FlinkKinesisProducer<String> kinesisOutputSink = getKinesisOutputSink(outputStreamName, region, "onekeyh7fdg8ffgt");
        
        // Add kinesis output2
        FlinkKinesisProducer<String> kinesisOutputSink2 = getKinesisOutputSink(outputStreamName, region, "twokeyh8fgdfdhh");

        // Add kinesis output3
        FlinkKinesisProducer<String> kinesisOutputSink3 = getKinesisOutputSink(outputStreamName, region, "NnPUkJ26jAaDM6w");

        // Add kinesis output4
        FlinkKinesisProducer<String> kinesisOutputSink4 = getKinesisOutputSink(outputStreamName, region, "yBxGACh5mNBJc48");

        // Add kinesis output5
        FlinkKinesisProducer<String> kinesisOutputSink5 = getKinesisOutputSink(outputStreamName, region, "Tg4TRZQb8hCAXav");

        // Add kinesis output6
        FlinkKinesisProducer<String> kinesisOutputSink6 = getKinesisOutputSink(outputStreamName, region, "Z7NwucB8rcQFJyj");

        // Add kinesis output7
        FlinkKinesisProducer<String> kinesisOutputSink7 = getKinesisOutputSink(outputStreamName, region, "j2YDqwAXqPHhxb3");

        // Add kinesis output8
        FlinkKinesisProducer<String> kinesisOutputSink8 = getKinesisOutputSink(outputStreamName, region, "6sRVeF3CVmBdN2K");

        // Add kinesis output9
        FlinkKinesisProducer<String> kinesisOutputSink9 = getKinesisOutputSink(outputStreamName, region, "k2upaW24bEur7tK");

        // Add kinesis output10
        FlinkKinesisProducer<String> kinesisOutputSink10 = getKinesisOutputSink(outputStreamName, region, "d6F4LKwHppzRBRe");

        // Add kinesis output11
        FlinkKinesisProducer<String> kinesisOutputSink11 = getKinesisOutputSink(outputStreamName, region, "qzkWxsxQ7Rrwu6B");

        // Add kinesis output12
        FlinkKinesisProducer<String> kinesisOutputSink12 = getKinesisOutputSink(outputStreamName, region, "2L7WVfL2gDNg8KR");

        // Add kinesis output13
        FlinkKinesisProducer<String> kinesisOutputSink13 = getKinesisOutputSink(outputStreamName, region, "vch4oqwWr75vwFj");

        // Add kinesis output14
        FlinkKinesisProducer<String> kinesisOutputSink14 = getKinesisOutputSink(outputStreamName, region, "m42DPQaDqqQFgmz");

        // Add kinesis output15
        FlinkKinesisProducer<String> kinesisOutputSink15 = getKinesisOutputSink(outputStreamName, region, "wfzZW83jk9tJFSw");

        // Add kinesis output16
        FlinkKinesisProducer<String> kinesisOutputSink16 = getKinesisOutputSink(outputStreamName, region, "WZbRh8HBeEhrTKx");

        // Add kinesis output17
        FlinkKinesisProducer<String> kinesisOutputSink17 = getKinesisOutputSink(outputStreamName, region, "VV9VEDtZ5d8aRhx");

        // Add kinesis output18
        FlinkKinesisProducer<String> kinesisOutputSink18 = getKinesisOutputSink(outputStreamName, region, "bAzFkWw6yd7bJqA");

        // Add kinesis output19
        FlinkKinesisProducer<String> kinesisOutputSink19 = getKinesisOutputSink(outputStreamName, region, "ZBy6XHwNsCooAJb");

        // Add kinesis output20
        FlinkKinesisProducer<String> kinesisOutputSink20 = getKinesisOutputSink(outputStreamName, region, "Dixow28Rfj7EesJ");

        // Add kinesis output21
        FlinkKinesisProducer<String> kinesisOutputSink21 = getKinesisOutputSink(outputStreamName, region, "9EyYLU8XnQ4eeBB");

        // Add kinesis output22
        FlinkKinesisProducer<String> kinesisOutputSink22 = getKinesisOutputSink(outputStreamName, region, "qbY8boo6ichKgaw");

        // Add kinesis output23
        FlinkKinesisProducer<String> kinesisOutputSink23 = getKinesisOutputSink(outputStreamName, region, "7A4qKwg6gUVkSGg");

        // Add kinesis output24
        FlinkKinesisProducer<String> kinesisOutputSink24 = getKinesisOutputSink(outputStreamName, region, "ce6VDAwLWHbh5Zs");

        // Add kinesis output25
        FlinkKinesisProducer<String> kinesisOutputSink25 = getKinesisOutputSink(outputStreamName, region, "6iv473DXatdMkJM");

        
        
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
        //convert json string to AppModel stream using a helper class
        DataStream<AppModel> inputAppModelStream = JsonToAppModelStream.convert(inputStream);

        //use table api, i.e. convert input stream to a table, use timestamp field as event time
        Table inputTable = tableEnv.fromDataStream(inputAppModelStream,
                "appName,appSessionId,version,processingTimestamp.rowtime");

        //use table api for Tumbling window then group by application name and emit result
        //Table outputTable = inputTable
        //        .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
        //        .groupBy("w, appName")
        //        .select("appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");
        
        //use table api for Tumbling window then group by application name and emit result
        Table outputTable = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q1' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");
  
        Table outputTable2 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q2' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");
        
        Table outputTable3 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q3' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable4 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q4' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable5 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q5' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable6 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q6' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable7 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q7' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable8 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q8' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable9 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q9' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable10 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q10' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable11 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q11' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable12 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q12' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable13 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q13' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable14 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q14' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable15 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q15' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable16 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q16' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable17 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q17' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable18 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q18' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable19 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q19' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable20 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q20' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable21 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q21' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable22 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q22' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable23 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q23' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable24 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q24' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        Table outputTable25 = inputTable
                .window(Tumble.over("1.minutes").on("processingTimestamp").as("w"))
                .groupBy("w, appName")
                .select("'q25' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");

        
        
        //write input to log4j sink for debugging
        //do not log input for scale test
        //inputTable.writeToSink(new Log4jTableSink("Input"));

        //do not write output for scale test, rather use CW metric sink
        //write output to log4j sink for debugging
        //outputTable.writeToSink(new Log4jTableSink("Output"));

        outputTable.writeToSink(new CWMetricTableSink(metricTag));

        //write output to kinesis stream
        //outputTable.writeToSink(new KinesisTableSink(kinesisOutputSink));
        
        KinesisTableSink mySink = new KinesisTableSink(kinesisOutputSink);
        KinesisTableSink mySink2 = new KinesisTableSink(kinesisOutputSink2);
        KinesisTableSink mySink3 = new KinesisTableSink(kinesisOutputSink3);
        KinesisTableSink mySink4 = new KinesisTableSink(kinesisOutputSink4);
        KinesisTableSink mySink5 = new KinesisTableSink(kinesisOutputSink5);
        KinesisTableSink mySink6 = new KinesisTableSink(kinesisOutputSink6);
        KinesisTableSink mySink7 = new KinesisTableSink(kinesisOutputSink7);
        KinesisTableSink mySink8 = new KinesisTableSink(kinesisOutputSink8);
        KinesisTableSink mySink9 = new KinesisTableSink(kinesisOutputSink9);
        KinesisTableSink mySink10 = new KinesisTableSink(kinesisOutputSink10);
        KinesisTableSink mySink11 = new KinesisTableSink(kinesisOutputSink11);
        KinesisTableSink mySink12 = new KinesisTableSink(kinesisOutputSink12);
        KinesisTableSink mySink13 = new KinesisTableSink(kinesisOutputSink13);
        KinesisTableSink mySink14 = new KinesisTableSink(kinesisOutputSink14);
        KinesisTableSink mySink15 = new KinesisTableSink(kinesisOutputSink15);
        KinesisTableSink mySink16 = new KinesisTableSink(kinesisOutputSink16);
        KinesisTableSink mySink17 = new KinesisTableSink(kinesisOutputSink17);
        KinesisTableSink mySink18 = new KinesisTableSink(kinesisOutputSink18);
        KinesisTableSink mySink19 = new KinesisTableSink(kinesisOutputSink19);
        KinesisTableSink mySink20 = new KinesisTableSink(kinesisOutputSink20);
        KinesisTableSink mySink21 = new KinesisTableSink(kinesisOutputSink21);
        KinesisTableSink mySink22 = new KinesisTableSink(kinesisOutputSink22);
        KinesisTableSink mySink23 = new KinesisTableSink(kinesisOutputSink23);
        KinesisTableSink mySink24 = new KinesisTableSink(kinesisOutputSink24);
        KinesisTableSink mySink25 = new KinesisTableSink(kinesisOutputSink25);
        
        outputTable.writeToSink(mySink);
        outputTable2.writeToSink(mySink2);
        outputTable3.writeToSink(mySink3);
        outputTable4.writeToSink(mySink4);
        outputTable5.writeToSink(mySink5);
        outputTable6.writeToSink(mySink6);
        outputTable7.writeToSink(mySink7);
        outputTable8.writeToSink(mySink8);
        outputTable9.writeToSink(mySink9);
        outputTable10.writeToSink(mySink10);
        outputTable11.writeToSink(mySink11);
        outputTable12.writeToSink(mySink12);
        outputTable13.writeToSink(mySink13);
        outputTable14.writeToSink(mySink14);
        outputTable15.writeToSink(mySink15);
        outputTable16.writeToSink(mySink16);
        outputTable17.writeToSink(mySink17);
        outputTable18.writeToSink(mySink18);
        outputTable19.writeToSink(mySink19);
        outputTable20.writeToSink(mySink20);
        outputTable21.writeToSink(mySink21);
        outputTable22.writeToSink(mySink22);
        outputTable23.writeToSink(mySink23);
        outputTable24.writeToSink(mySink24);
        outputTable25.writeToSink(mySink25);
        

        env.execute();
    }

    //private static FlinkKinesisProducer<String> getKinesisOutputSink(String outputStreamName, String region) {
    private static FlinkKinesisProducer<String> getKinesisOutputSink(String outputStreamName, String region, String partionKey) {
        Properties producerConfig = new Properties();
        // Required configs
        producerConfig.put(AWSConfigConstants.AWS_REGION, region);
        // Optional configs
        producerConfig.put("RecordTtl", "30000");
        producerConfig.put("AggregationEnabled", "false");
        producerConfig.put("RequestTimeout", "10000");
        producerConfig.put("ThreadingModel", "POOLED");
        producerConfig.put("ThreadPoolSize", "15");

        FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(new SimpleStringSchema(), producerConfig);
        kinesis.setFailOnError(true);
        kinesis.setDefaultStream(outputStreamName);
        kinesis.setDefaultPartition("partionKey");
        return kinesis;
    }

    private static DataStream<String> getInputDataStream(StreamExecutionEnvironment env, String inputStreamName, String region) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfigConstants.AWS_REGION, region);
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(
                inputStreamName, new SimpleStringSchema(), consumerConfig))
                .name("kinesis");
    }


    private static String getAppProperty(String name, final String defaultValue) {
        String value = defaultValue;
        if (appProperties != null) {
            value = appProperties.getProperty(name);
            value = StringUtils.isBlank(value) ? defaultValue : value;
        }
        return value;
    }

    private static int getAppPropertyInt(String name, final int defaultIntValue) {
        String value = getAppProperty(name, "" + defaultIntValue);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOG.error("invalid string value {} given for property {} using default value ", value, name);
            return defaultIntValue;
        }
    }

    // helper method to return runtime properties for Property Group AppProperties
    private static Properties initRuntimeConfigProperties() {
        try {
            Map<String, Properties> runConfigurations = KinesisAnalyticsRuntime.getApplicationProperties();
            return runConfigurations.get("AppProperties");
        } catch (IOException var1) {
            LOG.error("Could not retrieve the runtime config properties for {}, exception {}", "AppProperties", var1);
            return null;
        }
    }

}
