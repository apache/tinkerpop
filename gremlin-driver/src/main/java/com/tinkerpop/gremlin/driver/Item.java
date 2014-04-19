package com.tinkerpop.gremlin.driver;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Item {
    final Object resultItem;

    public Item(final Object resultItem) {
        this.resultItem = resultItem;
    }

    public <T> T get(final Class<? extends T> clazz) {
        return clazz.cast(this.resultItem);
    }
}
