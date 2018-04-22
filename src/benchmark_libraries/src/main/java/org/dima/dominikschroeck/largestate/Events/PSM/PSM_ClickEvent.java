package org.dima.dominikschroeck.largestate.Events.PSM;

import java.io.Serializable;

public class PSM_ClickEvent implements Serializable {
    public long timestamp;

    public String category;
    public String product;
    public String owner;
    public double price;
    public long ingestion_stamp;


    public PSM_ClickEvent() {
    }

    public PSM_ClickEvent(Long timestamp, String category, String product, String owner, Double price, Long ingestion_stamp) {
        this.timestamp = timestamp;
        this.category = category;
        this.product = product;
        this.owner = owner;
        this.price = price;
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

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getIngestion_stamp() {
        return ingestion_stamp;
    }

    public void setIngestion_stamp(long ingestion_stamp) {
        this.ingestion_stamp = ingestion_stamp;
    }
}
