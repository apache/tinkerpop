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

import org.apache.tinkerpop.machine.structure.data.TEdge;
import org.apache.tinkerpop.machine.structure.data.TKV;
import org.apache.tinkerpop.machine.structure.data.TVertex;
import org.apache.tinkerpop.machine.util.IteratorUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BlueprintsVertex implements TVertex, Serializable {

    @Override
    public Iterator<TEdge> inEdges() {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<TEdge> outEdges() {
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
    public void set(Object key, Object value) {

    }

    @Override
    public Object get(Object key) {
        return "marko";
    }

    @Override
    public Iterator keys() {
        return IteratorUtils.of("name");
    }

    @Override
    public Iterator values() {
        return IteratorUtils.of("marko");
    }

    @Override
    public Iterator<TKV> entries() {
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        return this.id().toString();
    }
}