package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.ResponseMessage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultSet implements Iterable<Item> {
    private final ResponseQueue responseQueue;

    public ResultSet(final ResponseQueue responseQueue) {
        this.responseQueue = responseQueue;
    }

    public boolean isFullyFetched() {
        return responseQueue.getStatus() == ResponseQueue.Status.COMPLETE;
    }

    public CompletableFuture<List<Item>> all() {
        return CompletableFuture.supplyAsync(() -> {
            final List<Item> list = new ArrayList<>();
            while (!isFullyFetched() || responseQueue.size() > 0) {
                final ResponseMessage msg = responseQueue.poll();
                if (msg != null)
                    list.add(new Item(msg.getResult()));
            }
            return list;
        });
    }

    @Override
    public Iterator<Item> iterator() {
        return new Iterator<Item>() {
            private Item current = null;

            @Override
            public boolean hasNext() {
                final ResponseMessage msg = responseQueue.poll();
                if (null == msg)
                    return false;

                // todo: wait??

                this.current = new Item(msg.getResult());
                return true;
            }

            @Override
            public Item next() {
                return current;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
