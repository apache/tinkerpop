/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import scala.Tuple2;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MapIterator<K, V> implements Iterator<Tuple2<K, V>> {

    private final Iterator<Tuple2<Object, VertexWritable>> inputIterator;
    private final MapReduce<K, V, ?, ?, ?> mapReduce;
    private final Queue<Tuple2<K, V>> queue = new LinkedList<>();
    private final MapIteratorEmitter mapIteratorEmitter = new MapIteratorEmitter();

    public MapIterator(final MapReduce<K, V, ?, ?, ?> mapReduce, final Iterator<Tuple2<Object, VertexWritable>> inputIterator) {
        this.inputIterator = inputIterator;
        this.mapReduce = mapReduce;
        this.mapReduce.workerStart(MapReduce.Stage.MAP);
    }


    @Override
    public boolean hasNext() {
        if (!this.queue.isEmpty())
            return true;
        else if (!this.inputIterator.hasNext()) {
            this.mapReduce.workerEnd(MapReduce.Stage.MAP);
            return false;
        } else {
            this.processNext();
            return this.hasNext();
        }
    }

    @Override
    public Tuple2<K, V> next() {
        if (!this.queue.isEmpty())
            return this.queue.remove();
        else if (!this.inputIterator.hasNext()) {
            this.mapReduce.workerEnd(MapReduce.Stage.MAP);
            throw FastNoSuchElementException.instance();
        } else {
            this.processNext();
            return this.next();
        }
    }

    private void processNext() {
        this.mapReduce.map(ComputerGraph.mapReduce(this.inputIterator.next()._2().get()), this.mapIteratorEmitter);
    }

    private class MapIteratorEmitter implements MapReduce.MapEmitter<K, V> {

        @Override
        public void emit(final K key, V value) {
            queue.add(new Tuple2<>(key, value));
        }
    }
}
