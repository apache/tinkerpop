package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerWorkerPool {

    private final int numberOfWorkers;
    private final List<MapReduce> mapReducers;

    public TinkerWorkerPool(final int numberOfWorkers, final Configuration configuration) {
        this.numberOfWorkers = numberOfWorkers;
        this.mapReducers = new ArrayList<>(this.numberOfWorkers);
        for (int i = 0; i < this.numberOfWorkers; i++) {
            this.mapReducers.add(MapReduce.createMapReduce(configuration));
        }
    }

    public void executeMapReduce(final Consumer<MapReduce> worker) {
        final CountDownLatch activeWorkers = new CountDownLatch(this.numberOfWorkers);
        for (final MapReduce mapReduce : this.mapReducers) {
            final Thread thread = new Thread(() -> {
                worker.accept(mapReduce);
                activeWorkers.countDown();
            });
            thread.start();
        }
        try {
            activeWorkers.await();
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}