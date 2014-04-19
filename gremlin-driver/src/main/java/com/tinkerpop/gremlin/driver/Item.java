package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.ResponseMessage;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Item {
    final Object resultItem;

    public Item(final ResponseMessage response) {
        this.resultItem = response.getResult();
    }

    public <T> T get(final Class<? extends T> clazz) {
        return clazz.cast(this.resultItem);
    }
}
