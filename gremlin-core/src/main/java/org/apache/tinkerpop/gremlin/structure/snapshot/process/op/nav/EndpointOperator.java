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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

/**
 * {@code outV}, {@code inV}, {@code bothV}: the end vertices of each edge. {@code BOTH} emits the out vertex, then the
 * in vertex.
 */
final class EndpointOperator extends EntryStreamOperator {

    private final Direction direction;
    private CsrSnapshot snapshot;

    EndpointOperator(final Ops.Endpoint node, final OperatorSpec spec) {
        super(spec);
        this.direction = node.direction();
    }

    @Override
    protected void onOpen() {
        snapshot = ctx.snapshot();
    }

    @Override
    protected int maxEmitPerEntry() {
        return direction == Direction.BOTH ? 2 : 1;
    }

    @Override
    protected boolean process(final Batch in, final int i, final Batch out) {
        final int edge = in.ord[i];
        final long bulk = in.bulk[i];
        if (direction != Direction.IN) out.addV(snapshot.edgeOut(edge), bulk);
        if (direction != Direction.OUT) out.addV(snapshot.edgeIn(edge), bulk);
        return true;
    }

    @Override
    protected void onClose() {
        snapshot = null;
    }
}
