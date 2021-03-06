package org.dima.dominikschroeck.largestate.KeySelectors;

import org.apache.flink.api.java.functions.KeySelector;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ImpressionEvent;

/**
 * KeySelector for Click and Impression evebts. Selects the Product ID (Field 3).
 */
public class Impression_Product_Key_Selector implements KeySelector<PSM_ImpressionEvent, String> {
    @Override
    public String getKey(PSM_ImpressionEvent Event) {
        return Event.getProduct();
    }
}
