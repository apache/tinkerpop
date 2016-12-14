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

package org.apache.tinkerpop.gremlin.structure;

import java.net.InetAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.UUID;

/**
 * A {@code Partition} represents a physical or logical split of the underlying {@link Graph} structure.
 * In distributed graph systems, a physical partition denotes which vertices/edges are in the subgraph of the underyling
 * physical machine. In a logical partition, a physical partition may be split amongst multiple threads and thus,
 * while isolated logically, they are united physically.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Partition {

    /**
     * Whether or not this element was, is, or will be contained in this partition.
     * Containment is not whether the element currently exists, but instead whether if it did exist, would it be
     * contained in this partition.
     *
     * @param element the element to check for containment
     * @return whether the element would be contained in this partition
     */
    public boolean contains(final Element element);

    /**
     * The current existing vertices contained in this partition.
     *
     * @param ids filtering to only those ids provided
     * @return an iterator of vertices contained in the partition
     */
    public Iterator<Vertex> vertices(final Object... ids);

    /**
     * The current existing edges contained in this partition.
     *
     * @param ids filtering to only those ids provided
     * @return an iterator of edges contained in the partition
     */
    public Iterator<Edge> edges(final Object... ids);

    /**
     * Get the {@link UUID} of the partition.
     *
     * @return the unique id of the partition
     */
    public UUID guid();

    /**
     * Get the {@link InetAddress} of the locations physical location.
     *
     * @return the physical location of the partition.
     */
    public InetAddress location();

    public static interface PhysicalPartition extends Partition {
    }

    public static interface LogicalPartition extends Partition {
    }
}