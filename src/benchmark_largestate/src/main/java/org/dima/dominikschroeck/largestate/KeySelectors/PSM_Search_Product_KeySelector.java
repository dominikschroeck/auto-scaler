package org.dima.dominikschroeck.largestate.KeySelectors;

import org.apache.flink.api.java.functions.KeySelector;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_SearchEvent;

/**
 * KeySelector for PSM's Search Event. Selects the Product ID as key (Field 3)
 */
public class PSM_Search_Product_KeySelector implements KeySelector<PSM_SearchEvent, String> {
    @Override
    public String getKey(PSM_SearchEvent value) {
        return value.getProduct();
    }
}