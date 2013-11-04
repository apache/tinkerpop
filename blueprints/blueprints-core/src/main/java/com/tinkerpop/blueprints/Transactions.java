package com.tinkerpop.blueprints;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author TinkerPop Community (http://tinkerpop.com)
 */
public interface Transactions extends Closeable {

    // TODO: create(), branch(), explicit(), context()

    public void open();

    public void commit();

    public void rollback();

    public <G extends Graph> G thread();

    public boolean isOpen();

    public default void readWrite() {
        if (!this.isOpen()) this.open();
    }

    public default void close() {
        if (this.isOpen()) this.commit();
    }

    public Transactions onReadWrite(Consumer<Transactions> consumer);

    public Transactions onClose(Consumer<Transactions> consumer);

}
