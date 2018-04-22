package org.dima.dominikschroeck.largestate.Events.NexMark;

import java.io.Serializable;

public class AuctionEvent implements Serializable {
    public Long timestamp;
    public Integer auction_id;
    public Integer person_id;
    public Integer item_id;
    public Double intialPrice;
    public Double reserve;
    public Long start;
    public Long end;
    public Long ingestion_timestamp;

    public AuctionEvent(Long timestamp, Integer auction_id, Integer person_id, Integer item_id, Double intialPrice, Double reserve, Long start, Long end, Long ingestion_timestamp) {
        this.timestamp = timestamp;
        this.auction_id = auction_id;
        this.person_id = person_id;
        this.item_id = item_id;
        this.intialPrice = intialPrice;
        this.reserve = reserve;
        this.start = start;
        this.end = end;
        this.ingestion_timestamp = ingestion_timestamp;
    }

    public AuctionEvent() {
    }

    public Long getIngestion_timestamp() {
        return ingestion_timestamp;
    }

    public void setIngestion_timestamp(Long ingestion_timestamp) {
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

    public Integer getItem_id() {
        return item_id;
    }

    public void setItem_id(Integer item_id) {
        this.item_id = item_id;
    }

    public Double getIntialPrice() {
        return intialPrice;
    }

    public void setIntialPrice(Double intialPrice) {
        this.intialPrice = intialPrice;
    }

    public Double getReserve() {
        return reserve;
    }

    public void setReserve(Double reserve) {
        this.reserve = reserve;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }
}
