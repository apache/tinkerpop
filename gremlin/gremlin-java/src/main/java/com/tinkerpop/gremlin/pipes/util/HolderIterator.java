package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.Pipeline;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderIterator<T> implements Iterator<Holder<T>> {

    private final Holder head;
    private final Iterator<T> iterator;
    private final Pipe pipe;

    public <P extends Pipeline> HolderIterator(final Iterator<T> iterator) {
        this.iterator = iterator;
        this.head = null;
        this.pipe = null;
    }

    public <P extends Pipeline> HolderIterator(final Pipe pipe, final Iterator<T> iterator) {
        this.iterator = iterator;
        this.head = null;
        this.pipe = pipe;
    }

    public <P extends Pipeline> HolderIterator(final Holder head, final Pipe pipe, final Iterator<T> iterator) {
        this.iterator = iterator;
        this.head = head.makeSibling();
        this.pipe = pipe;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Holder<T> next() {
        return null == this.head ?
                new Holder<>(null == this.pipe ? Pipe.NONE : this.pipe.getAs(), this.iterator.next()) :
                this.head.makeChild(null == this.pipe ? Pipe.NONE : this.pipe.getAs(), this.iterator.next());
    }
}
