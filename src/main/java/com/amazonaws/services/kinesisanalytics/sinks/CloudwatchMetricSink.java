/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesisanalytics.sinks;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import java.util.ArrayList;
import java.util.List;


import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Cloudwatch metric sink example, use this to write any numeric value as a metric to cloudwatch !
 * e.g. here we use CloudwatchMetricTupleModel providing us with various metric for App Events sample
 */
public class CloudwatchMetricSink extends RichSinkFunction<List<CloudwatchMetricTupleModel>> implements CheckpointedFunction {
    private static Logger LOG = LoggerFactory.getLogger(CloudwatchMetricSink.class);

    private final String flinkEventCategory;

    private long lastBufferFlush;
    private List<CloudwatchMetricTupleModel> values;
    private int batchSize;
    private long maxBufferTime;

    private transient AmazonCloudWatch cw;


    public CloudwatchMetricSink(String flinkEventCategory) {

        this.flinkEventCategory = flinkEventCategory;

        this.lastBufferFlush = System.currentTimeMillis();
        this.batchSize = 10;
        this.maxBufferTime = 10000;
        this.values = new ArrayList<CloudwatchMetricTupleModel>();


    }

    private void flushValuesBuffer()  {
        //send all data to cloudwatch
        Dimension dimensionEventCategory = new Dimension()
                .withName("EventCategory")
                .withValue(flinkEventCategory);

        values.forEach(v-> {
            Dimension dimensionGroupName = new Dimension()
                    .withName("AppName")
                    .withValue(v.getAppName());

            String metricName = v.getMetricName();
            MetricDatum datum = new MetricDatum()
                    .withMetricName(metricName)
                    .withUnit(StandardUnit.None)
                    .withValue(v.getMetricValue())
                    .withTimestamp( java.util.Date.from( v.getTimestamp().toInstant() ))
                    .withDimensions(dimensionEventCategory, dimensionGroupName);

            PutMetricDataRequest request = new PutMetricDataRequest()
                    .withNamespace("MyKinesisAnalytics/AppEvents")
                    .withMetricData(datum);
            PutMetricDataResult response = cw.putMetricData(request);
        });

        values.clear();
        lastBufferFlush = System.currentTimeMillis();
    }
    @Override
    public void invoke(List<CloudwatchMetricTupleModel> document)  {

        values.addAll(document);

        if (values.size() >= batchSize || System.currentTimeMillis() - lastBufferFlush >= maxBufferTime) {
            try {
                flushValuesBuffer();
            } catch (Exception e) {
                //if the request fails, that's fine, just retry on the next invocation
                LOG.error("Issue flushing CW metrics buffer : " + e.toString());
            }
        }
    }



    @Override
    public void open(Configuration configuration) throws Exception{
        super.open(configuration);

        this.lastBufferFlush = System.currentTimeMillis();
        this.batchSize = 100;
        this.maxBufferTime = 5000;
        this.values = new ArrayList<>();

        final AWSCredentialsProvider credentialsProvider  = new AWSCredentialsProviderChain(
                new SystemPropertiesCredentialsProvider(),
                new EnvironmentVariableCredentialsProvider(),
                new ProfileCredentialsProvider(),
                new InstanceProfileCredentialsProvider()
        );
        cw = AmazonCloudWatchClientBuilder.standard().withCredentials(credentialsProvider)
                .build();

    }


    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        do {
            try {
                flushValuesBuffer();
            } catch (Exception e) {
               LOG.error("snapshotState Issue flushing CW metric buffer : " + e.toString());

            }
        } while (! values.isEmpty());
    }


    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
