/*
 * Flink App Example
 * The app reads stream of app events and check min max and count of events reported
 *
 */

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.converters.JsonToAppModelStream;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
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

        // use event time for Time windows
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Add kinesis source
        // Notes: input data stream is a json formatted string
        DataStream<String> inputStream = getInputDataStream(env, inputStreamName, region);

        // Add kinesis output1
        FlinkKinesisProducer<String> kinesisOutputSink = getKinesisOutputSink(outputStreamName, region, "onekeyh7fdg8ffgt");
        
        // Add kinesis output2
        FlinkKinesisProducer<String> kinesisOutputSink2 = getKinesisOutputSink(outputStreamName, region, "twokeyh8fgdfdhh");
                
        

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
        //convert json string to AppModel stream using a helper class
        DataStream<AppModel> inputAppModelStream = JsonToAppModelStream.convert(inputStream);

        //use table api, i.e. convert input stream to a table, use timestamp field as event time
        Table inputTable = tableEnv.fromDataStream(inputAppModelStream,
                "appName,appSessionId,version,timestamp.rowtime");

        //use table api for Tumbling window then group by application name and emit result
        Table outputTable = inputTable
                .window(Tumble.over("1.minutes").on("timestamp").as("w"))
                .groupBy("w, appName")
                .select("'q1' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");
        
        //use table api for Tumbling window then group by application name and emit result
        Table outputTable2 = inputTable
                .window(Tumble.over("1.minutes").on("timestamp").as("w"))
                .groupBy("w, appName")
                .select("'q2' as queryid, appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");
     
        //write input to log4j sink for debugging
        inputTable.writeToSink(new Log4jTableSink("Input"));

        //write output to log4j sink for debugging
        outputTable.writeToSink(new Log4jTableSink("Output"));

        //write output to kinesis stream
        //outputTable.writeToSink(new KinesisTableSink(kinesisOutputSink));
        
        KinesisTableSink mySink = new KinesisTableSink(kinesisOutputSink);
        KinesisTableSink mySink2 = new KinesisTableSink(kinesisOutputSink2);
        
        outputTable.writeToSink(mySink);
        outputTable2.writeToSink(mySink2);
        
        env.execute();
    }

    private static FlinkKinesisProducer<String> getKinesisOutputSink(String outputStreamName, String region,String partionKey) {
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
        kinesis.setDefaultPartition(partionKey);
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
