/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.structure.util.partitioner;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GlobalPartitioner implements Partitioner {

    private final GlobalPartition partition;
    private transient Graph graph;
    private final Map<String, Object> graphConfiguration;

    public GlobalPartitioner(final Graph graph) {
        this.graph = graph;
        this.graphConfiguration = (Map) ConfigurationConverter.getMap(graph.configuration());
        this.partition = new GlobalPartition(this);
    }

    @Override
    public List<Partition> getPartitions() {
        return Collections.singletonList(this.partition);
    }

    @Override
    public Partition getPartition(final Element element) {
        return this.partition;
    }

    @Override
    public Graph getGraph() {
        if (null == this.graph)
            this.graph = GraphFactory.open(new MapConfiguration(this.graphConfiguration));
        return this.graph;
    }

    @Override
    public String toString() {
        return StringFactory.partitionerString(this);
    }

    private class GlobalPartition implements Partition {

        private final GlobalPartitioner partitioner;
        private final String id;
        private final InetAddress location;

        private GlobalPartition(final GlobalPartitioner partitioner) {
            this.partitioner = partitioner;
            this.id = "global-" + graph.getClass().getSimpleName().toLowerCase();
            try {
                this.location = InetAddress.getLocalHost();
            } catch (final UnknownHostException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public boolean contains(final Element element) {
            return true;
        }

        @Override
        public Iterator<Vertex> vertices(final Object... ids) {
            return this.partitioner.getGraph().vertices(ids);
        }

        @Override
        public Iterator<Edge> edges(final Object... ids) {
            return this.partitioner.getGraph().edges(ids);
        }

        @Override
        public String toString() {
            return StringFactory.partitionString(this);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Partition && ((Partition) other).id().equals(this.id);
        }

        @Override
        public int hashCode() {
            return this.id.hashCode() + this.location.hashCode();
        }

        @Override
        public String id() {
            return this.id;
        }

        @Override
        public InetAddress location() {
            return this.location;
        }

        @Override
        public Partitioner partitioner() {
            return this.partitioner;
        }
    }
}

