/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An immutable operator plan: a {@link CsrOp.Source}, then any number of ordinary nodes, then an optional
 * {@link CsrOp.Terminal}. Lanes are checked on construction by threading {@link CsrOp#outputLane(Lane)} through the
 * nodes, so a plan that exists is lane-consistent. A plan that starts with {@link Sources.Input} reads upstream
 * traversers (at the top level) or its parent's current batch (as a child).
 */
public final class CsrPlan {

    private final List<CsrOp> nodes;
    private final List<Lane> lanes;
    private final List<Boolean> sources;

    /**
     * @param nodes the source, the middle nodes and optionally the terminal, in order
     * @throws IllegalArgumentException if the shape or the lanes are inconsistent
     */
    public CsrPlan(final List<? extends CsrOp> nodes) {
        Objects.requireNonNull(nodes);
        if (nodes.isEmpty()) throw new IllegalArgumentException("A plan needs a source");
        this.nodes = List.copyOf(nodes);
        if (!(this.nodes.get(0) instanceof CsrOp.Source)) {
            throw new IllegalArgumentException("A plan must start with a source, not " + this.nodes.get(0).name());
        }
        final List<Lane> out = new ArrayList<>(this.nodes.size());
        final List<Boolean> recorded = new ArrayList<>(this.nodes.size());
        Lane lane = null;
        boolean source = false;
        for (int i = 0; i < this.nodes.size(); i++) {
            final CsrOp node = this.nodes.get(i);
            if (i > 0 && node instanceof CsrOp.Source && !(node instanceof Sources.MidScan)) {
                throw new IllegalArgumentException(node.name() + " can only start a plan");
            }
            if (node instanceof CsrOp.Terminal && i != this.nodes.size() - 1) {
                throw new IllegalArgumentException(node.name() + " can only end a plan");
            }
            if (node instanceof Ops.OtherV && !source) {
                throw new IllegalArgumentException("OtherV needs edges that recorded their source vertex");
            }
            lane = node.outputLane(lane);
            source = lane == Lane.E && node.outputRecordsSource(source);
            out.add(lane);
            recorded.add(source);
        }
        this.lanes = Collections.unmodifiableList(out);
        this.sources = Collections.unmodifiableList(recorded);
    }

    public static CsrPlan of(final CsrOp... nodes) {
        return new CsrPlan(List.of(nodes));
    }

    /**
     * All nodes, source first.
     */
    public List<CsrOp> nodes() {
        return nodes;
    }

    public CsrOp.Source source() {
        return (CsrOp.Source) nodes.get(0);
    }

    /**
     * The nodes between the source and the terminal.
     */
    public List<CsrOp> ops() {
        return nodes.subList(1, terminal() == null ? nodes.size() : nodes.size() - 1);
    }

    /**
     * The terminal, or null if the plan has none.
     */
    public CsrOp.Terminal terminal() {
        final CsrOp last = nodes.get(nodes.size() - 1);
        return last instanceof CsrOp.Terminal ? (CsrOp.Terminal) last : null;
    }

    /**
     * The lane the plan reads from upstream, or null if its source is not an {@link Sources.Input}.
     */
    public Lane inputLane() {
        return source() instanceof Sources.Input ? ((Sources.Input) source()).lane() : null;
    }

    /**
     * The lane after the last node.
     */
    public Lane outputLane() {
        return lanes.get(lanes.size() - 1);
    }

    /**
     * The lane after node {@code index}, so {@code laneAfter(0)} is the source's lane.
     */
    public Lane laneAfter(final int index) {
        return lanes.get(index);
    }

    /**
     * Whether batches after node {@code index} carry the source vertex of {@code E} entries.
     */
    public boolean recordsSourceAfter(final int index) {
        return sources.get(index);
    }

    /**
     * Whether the batches the plan emits carry the source vertex of {@code E} entries.
     */
    public boolean outputRecordsSource() {
        return sources.get(sources.size() - 1);
    }

    /**
     * Whether the plan ends in a terminal.
     */
    public boolean isReducing() {
        return terminal() != null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final CsrOp node : nodes) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(node);
        }
        return sb.toString();
    }
}
