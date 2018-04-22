package org.dima.dominikschroeck.largestate.Events.PSM;

import java.io.Serializable;

public class PSM_ImpressionEvent implements Serializable {
    public long timestamp;

    public String category;
    public String product;
    public long ingestion_stamp;


    public PSM_ImpressionEvent() {
    }

    public PSM_ImpressionEvent(long timestamp, String category, String product, long ingestion_stamp) {
        this.timestamp = timestamp;
        this.category = category;
        this.product = product;
        this.ingestion_stamp = ingestion_stamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public long getIngestion_stamp() {
        return ingestion_stamp;
    }

    public void setIngestion_stamp(long ingestion_stamp) {
        this.ingestion_stamp = ingestion_stamp;
    }
}
