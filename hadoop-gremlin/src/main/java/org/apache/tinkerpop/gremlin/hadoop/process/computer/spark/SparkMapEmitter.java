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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark;

import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

    private List<Tuple2<K, V>> emissions = new ArrayList<>();

    @Override
    public void emit(final K key, final V value) {
        this.emissions.add(new Tuple2<>(key, value));
    }

    public Iterator<Tuple2<K, V>> getEmissions() {
        final Iterator<Tuple2<K,V>> iterator = this.emissions.iterator();
        this.emissions = new ArrayList<>();
        return iterator;
    }
}
