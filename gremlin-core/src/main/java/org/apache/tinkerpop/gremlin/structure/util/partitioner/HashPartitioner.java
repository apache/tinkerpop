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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HashPartitioner implements Partitioner {

    private final List<Partition> partitions = new ArrayList<>();

    public HashPartitioner(final Partitioner basePartitioner, final int splits) {
        for (final Partition partition : basePartitioner.getPartitions()) {
            for (int i = 0; i < splits; i++) {
                this.partitions.add(new HashPartition(partition, i, splits));
            }
        }
    }

    @Override
    public String toString() {
        return StringFactory.partitionerString(this);
    }

    @Override
    public List<Partition> getPartitions() {
        return this.partitions;
    }

    @Override
    public Partition getPartition(final Element element) {
        for (final Partition partition : this.partitions) {
            if (partition.contains(element))
                return partition;
        }
        throw new IllegalArgumentException("The provided element is not in any known partition: " + element);
    }

    private static final class HashPartition implements Partition {

        private final Partition basePartition;
        private final int totalSplits;
        private final int splitId;
        private final String id;

        private HashPartition(final Partition basePartition, final int splitId, final int totalSplits) {
            this.basePartition = basePartition;
            this.totalSplits = totalSplits;
            this.splitId = splitId;
            this.id = this.basePartition.id() + "#" + splitId;
        }

        @Override
        public boolean contains(final Element element) {
            return (this.splitId == element.hashCode() % this.totalSplits) && this.basePartition.contains(element);
        }

        @Override
        public Iterator<Vertex> vertices(final Object... ids) {
            return IteratorUtils.filter(this.basePartition.vertices(ids), vertex -> this.splitId == vertex.hashCode() % this.totalSplits);
        }

        @Override
        public Iterator<Edge> edges(final Object... ids) {
            return IteratorUtils.filter(this.basePartition.edges(ids), edge -> this.splitId == edge.hashCode() % this.totalSplits);
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
            return this.id.hashCode() + this.location().hashCode();
        }

        @Override
        public String id() {
            return this.id;
        }

        @Override
        public InetAddress location() {
            return this.basePartition.location();
        }
    }
}

