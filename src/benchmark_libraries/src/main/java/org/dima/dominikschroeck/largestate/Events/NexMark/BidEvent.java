package org.dima.dominikschroeck.largestate.Events.NexMark;

import java.io.Serializable;

public class BidEvent implements Serializable {

    public Long timestamp;
    public Integer auction_id;
    public Integer person_id;
    public Integer bid_id;
    public Double bid;
    public Long ingestion_timestamp;

    public BidEvent() {
    }

    public BidEvent(Long timestamp, Integer auction_id, Integer person_id, Integer bid_id, Double bid, Long ingestion_timestamp) {
        this.timestamp = timestamp;
        this.auction_id = auction_id;
        this.person_id = person_id;
        this.bid_id = bid_id;
        this.bid = bid;
        this.ingestion_timestamp = ingestion_timestamp;
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

    public Integer getPerson_id() {
        return person_id;
    }

    public void setPerson_id(Integer person_id) {
        this.person_id = person_id;
    }

    public Integer getBid_id() {
        return bid_id;
    }

    public void setBid_id(Integer bid_id) {
        this.bid_id = bid_id;
    }

    public Double getBid() {
        return bid;
    }

    public void setBid(Double bid) {
        this.bid = bid;
    }

    public Long getIngestion_timestamp() {
        return ingestion_timestamp;
    }

    public void setIngestion_timestamp(Long ingestion_timestamp) {
        this.ingestion_timestamp = ingestion_timestamp;
    }
}