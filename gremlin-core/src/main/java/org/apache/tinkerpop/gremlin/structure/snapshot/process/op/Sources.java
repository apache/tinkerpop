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
import java.util.List;
import java.util.Objects;

/**
 * The nodes that start a plan: {@code Source := Scan(V|E) | Lookup(V|E, ids[]) | MidScan(V|E) | Input(lane)}.
 */
public final class Sources {

    private Sources() {
    }

    private static Lane elementLane(final String node, final Lane lane) {
        if (!Objects.requireNonNull(lane).isElement()) throw new IllegalArgumentException(node + " needs V or E");
        return lane;
    }

    /**
     * {@code g.V()} or {@code g.E()} without ids: every ordinal in ascending order, bulk 1.
     */
    public record Scan(Lane lane) implements CsrOp.Source {
        public Scan {
            elementLane("Scan", lane);
        }

        @Override
        public Lane outputLane() {
            return lane;
        }

        @Override
        public String toString() {
            return "Scan(" + lane + ")";
        }
    }

    /**
     * {@code g.V(ids)} or {@code g.E(ids)}: the elements with the given identifiers, in argument order with duplicates,
     * skipping nulls and identifiers that do not exist. Identifiers are literal, already stripped of {@code Element}
     * wrappers, and resolved through {@code vertexOrdinalCoerced} or {@code edgeOrdinalCoerced} when the operator binds.
     */
    public record Lookup(Lane lane, List<Object> ids) implements CsrOp.Source {
        public Lookup {
            elementLane("Lookup", lane);
            ids = new ArrayList<>(ids);
        }

        @Override
        public Lane outputLane() {
            return lane;
        }

        @Override
        public String toString() {
            return "Lookup(" + lane + "," + ids + ")";
        }
    }

    /**
     * Mid-traversal {@code V()} or {@code E()}: each input entry emits the full scan, so the output bulk of an ordinal
     * is the input bulk sum. It is the only source that can follow other nodes. The input lane is unrestricted.
     */
    public record MidScan(Lane lane) implements CsrOp.Source {
        public MidScan {
            elementLane("MidScan", lane);
        }

        @Override
        public Lane outputLane() {
            return lane;
        }

        @Override
        public Lane outputLane(final Lane input) {
            return lane;
        }

        @Override
        public String toString() {
            return "MidScan(" + lane + ")";
        }
    }

    /**
     * Reads the entries of the given lane from whatever feeds the pipeline: upstream traversers at the top level,
     * the parent operator's current batch in a child plan.
     */
    public record Input(Lane lane) implements CsrOp.Source {
        public Input {
            Objects.requireNonNull(lane);
            if (lane == Lane.SCALAR) throw new IllegalArgumentException("Input cannot read SCALAR");
        }

        @Override
        public Lane outputLane() {
            return lane;
        }

        @Override
        public String toString() {
            return "Input(" + lane + ")";
        }
    }
}
