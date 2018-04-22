package org.dima.dominikschroeck.largestate.Events.PSM;

import java.io.Serializable;

public class PSM_SearchEvent implements Serializable {
    public long timestamp;
    public String category;
    public String product;
    public Integer rank;
    public long ingestion_stamp;


    public PSM_SearchEvent() {
    }

    public PSM_SearchEvent(long timestamp, String category, String product, Integer rank, long ingestion_stamp) {
        this.timestamp = timestamp;
        this.category = category;
        this.product = product;
        this.rank = rank;
        this.ingestion_stamp = ingestion_stamp;
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

    public long getTimestamp() {
        return timestamp;
    }

    public Integer getRank() {
        return rank;
    }

    public void setRank(Integer rank) {
        this.rank = rank;
    }
}
