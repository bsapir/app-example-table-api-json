package com.amazonaws.services.kinesisanalytics;

import com.google.gson.annotations.SerializedName;

import java.sql.Timestamp;

/**
 * App POJO class
 */
public class AppModel {
    // use this to avoid any serialization deserialization used within Flink
    public static final long serialVersionUID = 40L;

    public AppModel() {

    }

    public AppModel(String appName, String appId, Integer version, Timestamp timestamp) {
        this.appName = appName;
        this.appSessionId = appId;
        this.version = version;
        this.timestamp = timestamp;

    }

    @SerializedName("App Name")
    private String appName;

    @SerializedName("App Session ID")
    private String appSessionId;

    @SerializedName("App Version")
    private Integer version;

    @SerializedName("Timestamp")
    private Timestamp timestamp;

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