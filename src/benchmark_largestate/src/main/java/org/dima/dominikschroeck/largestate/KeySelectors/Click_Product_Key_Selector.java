package org.dima.dominikschroeck.largestate.KeySelectors;

import org.apache.flink.api.java.functions.KeySelector;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;

/**
 * KeySelector for Click and Impression evebts. Selects the Product ID (Field 3).
 */
public class Click_Product_Key_Selector implements KeySelector<PSM_ClickEvent, String> {
    @Override
    public String getKey(PSM_ClickEvent Event) {
        return Event.getProduct();
    }
}
