package com.amazonaws.services.kinesisanalytics;

import com.google.gson.annotations.SerializedName;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;

/**
 * App POJO class
 */
public class AppModel {
    // use this to avoid any serialization deserialization used within Flink
    public static final long serialVersionUID = 40L;

    public AppModel() {

    }

    public AppModel(String appName, String appId, Integer version, Timestamp timestamp, Timestamp processingTimestamp) {
        this.appName = appName;
        this.appSessionId = appId;
        this.version = version;
        this.timestamp = timestamp;
        this.processingTimestamp = processingTimestamp;

    }

    @SerializedName("App Name")
    private String appName;

    @SerializedName("App Session ID")
    private String appSessionId;

    @SerializedName("App Version")
    private Integer version;

    @SerializedName("Timestamp")
    private Timestamp timestamp;

    private Timestamp processingTimestamp;

    public void setAppSessionId(String appSessionId) {
        this.appSessionId = appSessionId;
    }

    public String getAppSessionId() {
        return this.appSessionId;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppName() {
        return this.appName;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public void setProcessingTimestamp(Timestamp timestamp) {
        this.processingTimestamp = timestamp;
    }

    /**
     * helper method to initialize processing timestamp while deserializing input json
     * @param processingTimestamp
     * @return
     */
    public AppModel withProcessingTime(Timestamp processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
        return this;
    }

    public Timestamp getProcessingTimestamp() {
        return this.processingTimestamp;
    }

    public Timestamp getTimestamp() {
        return this.timestamp;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Integer getVersion() {
        return this.version;
    }

    public String toString() {
        return "App Name: " + appName + " Timestamp: " + timestamp + " App Session ID: " + appSessionId + " App Version: " + version;
    }


}