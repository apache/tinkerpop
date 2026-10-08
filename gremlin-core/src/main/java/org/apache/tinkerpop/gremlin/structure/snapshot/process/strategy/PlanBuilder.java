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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.strategy;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

import java.util.ArrayList;
import java.util.List;

/**
 * The nodes of a plan under construction, with the lane after the last node. A compatible node may continue from a
 * terminal's output. Copies are cheap, so a composite rule can build on a copy and commit it only when every piece
 * compiled.
 */
final class PlanBuilder {

    private final List<CsrOp> nodes = new ArrayList<>();
    private Lane lane;
    private boolean recordsSource;
    private boolean hardClosed;

    PlanBuilder(final CsrOp.Source source) {
        add(source);
    }

    private PlanBuilder(final PlanBuilder other) {
        this.nodes.addAll(other.nodes);
        this.lane = other.lane;
        this.recordsSource = other.recordsSource;
        this.hardClosed = other.hardClosed;
    }

    PlanBuilder copy() {
        return new PlanBuilder(this);
    }

    /**
     * Takes over the state of a copy that was extended successfully.
     */
    void commit(final PlanBuilder copy) {
        nodes.clear();
        nodes.addAll(copy.nodes);
        lane = copy.lane;
        recordsSource = copy.recordsSource;
        hardClosed = copy.hardClosed;
    }

    /**
     * Appends a node.
     *
     * @throws Reject if the plan was explicitly closed or the node cannot read the current lane
     */
    void add(final CsrOp node) {
        if (hardClosed) throw new Reject("the plan cannot continue after a side-effect writer");
        try {
            lane = node.outputLane(nodes.isEmpty() ? null : lane);
        } catch (final IllegalArgumentException e) {
            throw new Reject(e.getMessage());
        }
        recordsSource = lane == Lane.E && node.outputRecordsSource(recordsSource);
        nodes.add(node);
    }

    /**
     * Replaces the node at the index and re-threads the lanes.
     */
    void replace(final int index, final CsrOp node) {
        final List<CsrOp> old = new ArrayList<>(nodes);
        nodes.clear();
        hardClosed = false;
        for (int i = 0; i < old.size(); i++) add(i == index ? node : old.get(i));
    }

    List<CsrOp> nodes() {
        return nodes;
    }

    int size() {
        return nodes.size();
    }

    Lane lane() {
        return lane;
    }

    boolean recordsSource() {
        return recordsSource;
    }

    /**
     * Ends the plan after the last node without a terminal.
     */
    void close() {
        hardClosed = true;
    }

    boolean endsInTerminal() {
        return nodes.get(nodes.size() - 1) instanceof CsrOp.Terminal;
    }

    boolean hasOps() {
        return nodes.size() > 1;
    }

    /**
     * Whether the plan touches enough of the graph to be worth running natively: anything beyond its source other than
     * bulk merging and ranges.
     */
    boolean isWorthwhile() {
        for (int i = 1; i < nodes.size(); i++) {
            final CsrOp node = nodes.get(i);
            if (!(node instanceof Ops.Merge) && !(node instanceof Ops.Range)) return true;
        }
        return false;
    }

    /**
     * @throws Reject if the plan is not lane-consistent
     */
    CsrPlan build() {
        try {
            return new CsrPlan(nodes);
        } catch (final IllegalArgumentException e) {
            throw new Reject(e.getMessage());
        }
    }
}
