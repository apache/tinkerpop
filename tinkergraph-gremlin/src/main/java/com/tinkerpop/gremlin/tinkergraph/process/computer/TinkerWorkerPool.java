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

    private final List<MapReduce> mapReducers;

    public TinkerWorkerPool(final int numberOfWorkers, final Configuration configuration) {
        this.mapReducers = new ArrayList<>(numberOfWorkers);
        for (int i = 0; i < numberOfWorkers; i++) {
            this.mapReducers.add(MapReduce.createMapReduce(configuration));
        }
    }

    public void executeMapReduce(final Consumer<MapReduce> worker) {
        final CountDownLatch activeWorkers = new CountDownLatch(this.mapReducers.size());
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