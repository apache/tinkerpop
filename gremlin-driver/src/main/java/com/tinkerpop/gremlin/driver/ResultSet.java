package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.util.StreamFactory;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A {@code ResultSet} is returned from the submission of a Gremlin script to the server and represents the
 * results provided by the server.  The results from the server are streamed into the {@code ResultSet} and
 * therefore may not be available immediately.  As such, {@code ResultSet} provides access to a a number
 * of functions that help to work with the asynchronous nature of the data streaming back.  Data from results
 * is stored in an {@link Result} which can be used to retrieve the item once it is on the client side.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultSet implements Iterable<Result> {
    private final ResponseQueue responseQueue;
    private final ExecutorService executor;
    private final Channel channel;
    private final Supplier<Void> onChannelError;

    public ResultSet(final ResponseQueue responseQueue, final ExecutorService executor,
                     final Channel channel, final Supplier<Void> onChannelError) {
        this.executor = executor;
        this.responseQueue = responseQueue;
        this.channel = channel;
        this.onChannelError = onChannelError;
    }

    /**
     * Determines if all items have been returned to the client.
     */
    public boolean allItemsAvailable() {
        return responseQueue.getStatus() == ResponseQueue.Status.COMPLETE;
    }

    /**
     * Gets the number of items available on the client.
     */
    public int getAvailableItemCount() {
        return responseQueue.size();
    }

    /**
     * Determines if there are any remaining items being streamed to the client.
     */
    public boolean isExhausted() {
        if (!responseQueue.isEmpty())
            return false;

        awaitItems(1).join();

        assert !responseQueue.isEmpty() || allItemsAvailable();
        return responseQueue.isEmpty();
    }

    /**
     * Get the next {@link Result} from the stream, blocking until one is available.
     */
    public Result one() {
        ResponseMessage msg = responseQueue.poll();
        if (msg != null)
            return new Result(msg);

        awaitItems(1).join();

        msg = responseQueue.poll();
        if (msg != null)
            return new Result(msg);
        else
            return null;
    }

    /**
     * Wait for some number of items to be available on the client. The future will contain the number of items
     * available which may or may not be the number the caller was waiting for.
     */
    public CompletableFuture<Integer> awaitItems(final int items) {
        if (allItemsAvailable())
            CompletableFuture.completedFuture(getAvailableItemCount());

        return CompletableFuture.supplyAsync(() -> {
            while (!allItemsAvailable() && getAvailableItemCount() < items) {
                if (!channel.isOpen()) {
                    onChannelError.get();
                    throw new RuntimeException("Error while processing results from channel - check client and server logs for more information");
                }

                try {
                    // small delay between checks for available items
                    Thread.sleep(10);
                } catch (Exception ex) {
                    return null;
                }
            }

            return getAvailableItemCount();
        }, executor);
    }

    /**
     * Wait for all items to be available on the client exhausting the stream.
     */
    public CompletableFuture<List<Result>> all() {
        return CompletableFuture.supplyAsync(() -> {
            final List<Result> list = new ArrayList<>();
            while (!isExhausted()) {
                final ResponseMessage msg = responseQueue.poll();
                if (msg != null)
                    list.add(new Result(msg));
            }
            return list;
        }, executor);
    }

    /**
     * Stream items with a blocking iterator.
     */
    public Stream<Result> stream() {
        return StreamFactory.stream(iterator());
    }

    @Override
    public Iterator<Result> iterator() {
        return new Iterator<Result>() {

            @Override
            public boolean hasNext() {
                return !isExhausted();
            }

            @Override
            public Result next() {
                return ResultSet.this.one();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
