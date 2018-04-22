package org.dima.dominikschroeck.largestate.Events.NexMark;

import java.io.Serializable;

public class AuctionEvent_Category implements Serializable {
    public Long timestamp;
    public Integer auction_id;
    public Double intialPrice;
    public Long end;
    public Integer category_id;
    public Long ingestion_timestamp;


    //input.auction_id, input.end, input.category_id, input.intialPrice,input.ingestion_timestamp)
    public AuctionEvent_Category(Long timestamp, Integer auction_id, Double intialPrice, Long end, Integer category_id, Long ingestion_timestamp) {
        this.timestamp = timestamp;
        this.auction_id = auction_id;
        this.intialPrice = intialPrice;
        this.end = end;
        this.category_id = category_id;
        this.ingestion_timestamp = ingestion_timestamp;


    }

    public AuctionEvent_Category() {
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getAuction_id() {
        return auction_id;
    }

    public void setAuction_id(Integer auction_id) {
        this.auction_id = auction_id;
    }

    public Double getIntialPrice() {
        return intialPrice;
    }

    public void setIntialPrice(Double intialPrice) {
        this.intialPrice = intialPrice;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public Integer getCategory_id() {
        return category_id;
    }

    public void setCategory_id(Integer category_id) {
        this.category_id = category_id;
    }

    public Long getIngestion_timestamp() {
        return ingestion_timestamp;
    }

    public void setIngestion_timestamp(Long ingestion_timestamp) {
        this.ingestion_timestamp = ingestion_timestamp;
    }




}
