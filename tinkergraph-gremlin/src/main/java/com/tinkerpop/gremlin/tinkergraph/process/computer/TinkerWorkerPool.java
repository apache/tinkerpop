package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerWorkerPool {

    private BlockingQueue<MapReduce> mapReduces = new LinkedBlockingQueue<>();

    public TinkerWorkerPool(final Configuration configuration, final int numberOfWorkers) {
        for (int i = 0; i < numberOfWorkers; i++) {
            this.mapReduces.add(MapReduce.createMapReduce(configuration));
        }
    }

    public void map(final Vertex vertex, final MapReduce.MapEmitter emitter) {
        try {
            final MapReduce mapReduce = this.mapReduces.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            mapReduce.map(vertex, emitter);
            this.mapReduces.put(mapReduce);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void reduce(final Object key, final Iterator<?> values, final MapReduce.ReduceEmitter emitter) {
        try {
            final MapReduce mapReduce = this.mapReduces.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            mapReduce.reduce(key, values, emitter);
            this.mapReduces.put(mapReduce);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}