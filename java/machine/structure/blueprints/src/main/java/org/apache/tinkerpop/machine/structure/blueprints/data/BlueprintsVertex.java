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
package org.apache.tinkerpop.machine.structure.blueprints.data;

import org.apache.tinkerpop.machine.structure.util.T2Tuple;
import org.apache.tinkerpop.machine.structure.TSequence;
import org.apache.tinkerpop.machine.structure.graph.TEdge;
import org.apache.tinkerpop.machine.structure.graph.TVertex;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BlueprintsVertex<V> implements TVertex<V>, Serializable {

    @Override
    public Iterator<TEdge<V>> inE() {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<TEdge<V>> outE() {
        return Collections.emptyIterator();
    }

    @Override
    public String label() {
        return "person";
    }

    @Override
    public Object id() {
        return UUID.randomUUID();
    }

    @Override
    public void set(String key, V value) {

    }

    @Override
    public void add(String key, V value) {

    }

    @Override
    public void remove(String key) {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public V value(String key) {
        return (V) "marko";
    }

    @Override
    public V value(String key, V defaultValue) {
        return defaultValue;
    }

    @Override
    public boolean has(String key) {
        return true;
    }

    @Override
    public Iterator<T2Tuple<String, V>> entries() {
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        return this.id().toString();
    }
}