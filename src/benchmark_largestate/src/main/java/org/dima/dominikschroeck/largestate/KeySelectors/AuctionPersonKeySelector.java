package org.dima.dominikschroeck.largestate.KeySelectors;

import org.apache.flink.api.java.functions.KeySelector;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;

// timestamp + "," + auction_id + "," + person_id + "," + item_id + "," + initialPrice + "," +  reserve + "," + start + "," + end;

/**
 * KeySelector for selecting the Person's ID out of an auction event
 */
public class AuctionPersonKeySelector implements KeySelector<AuctionEvent, Integer> {
    @Override
    public Integer getKey(AuctionEvent value) {
        return value.getPerson_id();
    }
}