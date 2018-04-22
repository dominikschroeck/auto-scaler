package org.dima.dominikschroeck.largestate.KeySelectors;

import org.apache.flink.api.java.functions.KeySelector;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewPersonEvent;

//  Person:  timestamp + "," + id + "," + email + "," + name + "," + credit_card + "," + city + "," + state ;

/**
 * KeySelector for a person tuple. Selects the person's ID
 */
public class PersonKeySelector implements KeySelector<NewPersonEvent, Integer> {
    @Override
    public Integer getKey(NewPersonEvent value) {
        return value.getPerson_id();
    }
}