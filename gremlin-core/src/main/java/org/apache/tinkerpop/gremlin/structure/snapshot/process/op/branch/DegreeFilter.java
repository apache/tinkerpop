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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.branch;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;

import java.util.Arrays;
import java.util.Objects;

/**
 * {@code filter(outE().count().is(P))} and its variants ({@code out()}, {@code in()}, {@code both()}, and the edge
 * forms): keeps the vertices whose number of adjacent edges, counted like an {@code Expand} with the same direction
 * and labels emits them, satisfies the predicate. Not a plan terminal, so the child {@code Count} and its {@code is}
 * never run; the degree comes from the adjacency offsets, or from a label scan of the adjacency. The entry passes with
 * its own bulk. The predicate is read when the operator opens.
 *
 * @param direction  the direction of the adjacency that is counted
 * @param labelCodes the edge label codes, null for all labels, empty for none
 * @param predicate  tested against the degree as a {@code Long}
 */
public record DegreeFilter(Direction direction, int[] labelCodes, P<?> predicate) implements CsrOp {

    public DegreeFilter {
        Objects.requireNonNull(direction);
        Objects.requireNonNull(predicate);
        labelCodes = labelCodes == null ? null : labelCodes.clone();
    }

    @Override
    public int[] labelCodes() {
        return labelCodes == null ? null : labelCodes.clone();
    }

    @Override
    public Lane outputLane(final Lane input) {
        if (input != Lane.V) throw CsrOp.badLane(this, input);
        return Lane.V;
    }

    @Override
    public String toString() {
        return "DegreeFilter(" + direction + "," + (labelCodes == null ? "*" : Arrays.toString(labelCodes)) + ","
                + predicate + ")";
    }
}
