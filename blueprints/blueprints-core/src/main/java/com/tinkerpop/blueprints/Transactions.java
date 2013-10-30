package com.tinkerpop.blueprints;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author TinkerPop Community (http://tinkerpop.com)
 */
public interface Transactions {

    public void open();

    public void commit();

    public void rollback();

    public <G extends Graph> G thread();

    public boolean isOpen();

    public default Transactions onReadWrite(Consumer<Transactions> consumer) {
        if (!this.isOpen())
            this.open();
        return this;
    }

    public default Transactions onClose(Consumer<Transactions> consumer) {
        this.commit();
        return this;
    }

    public default Transactions configure(Consumer<Transactions> consumer) {
        consumer.accept(this);
        return this;
    }

}
