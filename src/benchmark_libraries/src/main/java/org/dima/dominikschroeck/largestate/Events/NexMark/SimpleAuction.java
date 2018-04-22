package org.dima.dominikschroeck.largestate.Events.NexMark;

import java.io.Serializable;

public class SimpleAuction implements Serializable {
    public Integer auction_id;
    public Double intialPrice;
    public Long end;

    public SimpleAuction(Integer auction_id, Double intialPrice, Long end) {
        this.auction_id = auction_id;
        this.intialPrice = intialPrice;
        this.end = end;
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
}
