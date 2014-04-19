package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultSet implements Iterable<Item> {
    private final ResponseQueue responseQueue;

    public ResultSet(final ResponseQueue responseQueue) {
        this.responseQueue = responseQueue;
    }

    public boolean allItemsAvailable() {
        return responseQueue.getStatus() == ResponseQueue.Status.COMPLETE;
    }

    public int getAvailableItemCount() {
        return responseQueue.size();
    }

    public boolean isExhausted() {
        if (!responseQueue.isEmpty())
            return false;

        try {
            awaitItems(1).get();
        } catch (Exception ex) {
            // todo: block here...kinda have to
        }

        assert !responseQueue.isEmpty() || allItemsAvailable();
        return responseQueue.isEmpty();
    }

    public Item one() {
        ResponseMessage msg = responseQueue.poll();
        if (msg != null)
            return new Item(msg);

        awaitItems(1);

        msg = responseQueue.poll();
        if (msg != null)
            return new Item(msg);
        else
            return null;
    }

    public CompletableFuture<Void> awaitItems(final int items) {
        if (allItemsAvailable())
            CompletableFuture.completedFuture(null);

        return CompletableFuture.supplyAsync(() -> {
            while (!allItemsAvailable() && getAvailableItemCount() < items) {
                try {
                    Thread.sleep(10);
                } catch (Exception ex) {
                    return null; // todo: dumb?
                }
            }

            return null;
        });
    }

    public CompletableFuture<List<Item>> all() {
        return CompletableFuture.supplyAsync(() -> {
            final List<Item> list = new ArrayList<>();
            while (!isExhausted()) {
                final ResponseMessage msg = responseQueue.poll();
                if (msg != null)
                    list.add(new Item(msg));
            }
            return list;
        });
    }

    public Stream<Item> stream() {
        return StreamFactory.stream(iterator());
    }

    @Override
    public Iterator<Item> iterator() {
        return new Iterator<Item>() {

            @Override
            public boolean hasNext() {
                return !isExhausted();
            }

            @Override
            public Item next() {
                return ResultSet.this.one();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
