package com.amazonaws.services.kinesisanalytics.sinks;

import java.sql.Timestamp;

/**
 * A POJO for emitting time window aggregation output to cloudwatch metric
 */
public class CloudwatchMetricTupleModel {
    // use this to avoid any serialization deserialization used within Flink
    public static final long serialVersionUID = 50L;
    public CloudwatchMetricTupleModel(String appName, String metricName, Timestamp timestamp, Double metricValue) {
        this.appName = appName;
        this.metricName = metricName;
        this.timestamp = timestamp;
        this.metricValue = metricValue;
    }

    public CloudwatchMetricTupleModel() {

    }

    private String appName;
    private String metricName;
    private Double metricValue;
    private Timestamp timestamp;


    public void setAppName(String name) {
        this.appName = name;
    }

    public String getAppName() {
        return this.appName;
    }

    public Double getMetricValue () {
        return this.metricValue;
    }

    public void setMetricValue(Double value) {
        this.metricValue = value;
    }

    public void setMetricName(String name) {
        this.metricName = name;
    }

    public String getMetricName() {
        return this.metricName;
    }

    public void setTimestamp(Timestamp ts) {
        this.timestamp = ts;
    }

    public Timestamp getTimestamp() {
        return this.timestamp;
    }

}
