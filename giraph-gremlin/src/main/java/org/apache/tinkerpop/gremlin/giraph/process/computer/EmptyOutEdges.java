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
package org.apache.tinkerpop.gremlin.giraph.process.computer;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.NullWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyOutEdges implements OutEdges<ObjectWritable, NullWritable> {

    private static final EmptyOutEdges INSTANCE = new EmptyOutEdges();

    public static EmptyOutEdges instance() {
        return INSTANCE;
    }

    @Override
    public void initialize(final Iterable<Edge<ObjectWritable, NullWritable>> edges) {
    }

    @Override
    public void initialize(final int capacity) {
    }

    @Override
    public void initialize() {
    }

    @Override
    public void add(final Edge<ObjectWritable, NullWritable> edge) {
    }

    @Override
    public void remove(final ObjectWritable targetVertexId) {
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Iterator<Edge<ObjectWritable, NullWritable>> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException {
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
    }
}
