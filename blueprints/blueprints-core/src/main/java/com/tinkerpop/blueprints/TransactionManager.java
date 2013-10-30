package com.tinkerpop.blueprints;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TransactionManager {

    public void open();

    public void commit();

    public void rollback();

    public boolean isOpen();

    public <G extends Graph> G thread();

    public default TransactionManager onClose(Consumer<TransactionManager> consumer) {
        this.commit();
        return this;
    }

    public default TransactionManager configure(Consumer<TransactionManager> consumer) {
        consumer.accept(this);
        return this;
    }

}
