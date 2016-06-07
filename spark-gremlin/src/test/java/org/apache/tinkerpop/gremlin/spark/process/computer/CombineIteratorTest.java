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

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CombineIteratorTest {

    @Test
    public void shouldBulkResults() {
        long total = 0;
        final List<Tuple2<String, Long>> numbers = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            total = total + i;
            numbers.add(new Tuple2<>("test", i));
        }

        final CombineIterator<String, Long, String, Long> combineIterator = new CombineIterator<>(new MapReduceA(), numbers.iterator());
        final Tuple2<String, Long> tuple = combineIterator.next();
        assertEquals("test", tuple._1());
        assertEquals(Long.valueOf(total), tuple._2());
        assertFalse(combineIterator.hasNext());
    }

    @Test
    public void shouldDoubleBulkResults() {
        long total = 0;
        final List<Tuple2<String, Long>> numbers = new ArrayList<>();
        for (long i = 0; i < 9000; i++) {
            total = total + i;
            numbers.add(new Tuple2<>("test", i));
        }

        final CombineIterator<String, Long, String, Long> combineIterator = new CombineIterator<>(new MapReduceA(), numbers.iterator());
        assertTrue(combineIterator.hasNext());
        final Tuple2<String, Long> tuple = combineIterator.next();
        assertFalse(combineIterator.hasNext());
        assertEquals("test", tuple._1());
        assertEquals(total, tuple._2().longValue());
    }

    @Test
    public void shouldTripleBulkResults() {
        long total = 0;
        final List<Tuple2<String, Long>> numbers = new ArrayList<>();
        for (long i = 0; i < 14000; i++) {
            total = total + i;
            numbers.add(new Tuple2<>("test", i));
        }

        final CombineIterator<String, Long, String, Long> combineIterator = new CombineIterator<>(new MapReduceA(), numbers.iterator());
        assertTrue(combineIterator.hasNext());
        final Tuple2<String, Long> tuple = combineIterator.next();
        assertFalse(combineIterator.hasNext());
        assertEquals("test", tuple._1());
        assertEquals(total, tuple._2().longValue());
    }

    @Test
    public void shouldEndlessBulkResults() {
        long total = 0;
        final List<Tuple2<String, Long>> numbers = new ArrayList<>();
        for (long i = 0; i < 5000000; i++) {
            total = total + i;
            numbers.add(new Tuple2<>("test", i));
        }

        final CombineIterator<String, Long, String, Long> combineIterator = new CombineIterator<>(new MapReduceA(), numbers.iterator());
        assertTrue(combineIterator.hasNext());
        final Tuple2<String, Long> tuple = combineIterator.next();
        assertFalse(combineIterator.hasNext());
        assertEquals("test", tuple._1());
        assertEquals(total, tuple._2().longValue());
    }

    @Test
    public void shouldEndlessBulkResultsWithNullObject() {
        long total = 0;
        final List<Tuple2<MapReduce.NullObject, Long>> numbers = new ArrayList<>();
        for (long i = 0; i < 5000000; i++) {
            total = total + i;
            numbers.add(new Tuple2<>(MapReduce.NullObject.instance(), i));
        }

        final CombineIterator<MapReduce.NullObject, Long, MapReduce.NullObject, Long> combineIterator = new CombineIterator<>(new MapReduceB(), numbers.iterator());
        assertTrue(combineIterator.hasNext());
        final Tuple2<MapReduce.NullObject, Long> tuple = combineIterator.next();
        assertFalse(combineIterator.hasNext());
        assertEquals(MapReduce.NullObject.instance(), tuple._1());
        assertEquals(total, tuple._2().longValue());
    }

    @Test
    public void shouldBulkResultsByKey() {
        long total = 0;
        final List<Tuple2<String, Long>> numbers = new ArrayList<>();
        for (long i = 0; i < 9000; i++) {
            total = total + i;
            numbers.add(new Tuple2<>(UUID.randomUUID().toString(), i));
        }

        final CombineIterator<String, Long, String, Long> combineIterator = new CombineIterator<>(new MapReduceA(), numbers.iterator());
        assertEquals(9000, IteratorUtils.count(combineIterator));
    }

    private static class MapReduceA extends StaticMapReduce<String, Long, String, Long, Long> {

        @Override
        public void combine(final String key, final Iterator<Long> values, final ReduceEmitter<String, Long> emitter) {
            long counter = 0;
            while (values.hasNext()) {
                counter = counter + values.next();
            }
            emitter.emit(key, counter);
        }



        @Override
        public boolean doStage(final Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<String, Long> emitter) {

        }

        @Override
        public String getMemoryKey() {
            return "test";
        }

        @Override
        public Long generateFinalResult(final Iterator<KeyValue<String, Long>> keyValues) {
            return keyValues.next().getValue();
        }
    }

    private static class MapReduceB extends StaticMapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> {

        @Override
        public void combine(final MapReduce.NullObject key, final Iterator<Long> values, final ReduceEmitter<MapReduce.NullObject, Long> emitter) {
            long counter = 0;
            while (values.hasNext()) {
                counter = counter + values.next();
            }
            emitter.emit(key, counter);
        }

        @Override
        public boolean doStage(final Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Long> emitter) {

        }

        @Override
        public String getMemoryKey() {
            return "test";
        }

        @Override
        public Long generateFinalResult(final Iterator<KeyValue<MapReduce.NullObject, Long>> keyValues) {
            return keyValues.next().getValue();
        }
    }
}
