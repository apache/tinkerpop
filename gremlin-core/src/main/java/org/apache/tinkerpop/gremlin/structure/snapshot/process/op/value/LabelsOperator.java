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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.value;

import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;

import java.util.List;

/**
 * {@code labels()}: the labels of a vertex in source order, repeated labels once as in {@code Vertex#labels()}, or the
 * label of an edge. A multi-label vertex without labels emits nothing.
 */
final class LabelsOperator extends FanOutOperator {

    private final boolean vertices;
    private CsrSnapshot snapshot;
    private List<String> dictionary;
    private int index;
    private int count;

    LabelsOperator(final OperatorSpec spec) {
        super(spec);
        this.vertices = spec.inputLane() == Lane.V;
    }

    @Override
    protected void onOpen() {
        snapshot = ctx.snapshot();
        dictionary = vertices ? snapshot.vertexLabels() : null;
    }

    @Override
    protected void begin(final Batch in, final int i) {
        index = 0;
        count = vertices ? snapshot.vertexLabelCount(in.ord[i]) : 1;
    }

    @Override
    protected boolean emit(final Batch in, final int i, final Batch out) {
        final int owner = in.ord[i];
        if (!vertices) {
            out.addValue(snapshot.edgeLabel(owner), in.bulk[i]);
            index = 1;
            return true;
        }
        while (index < count) {
            if (out.isFull()) return false;
            final int code = snapshot.vertexLabelCodeAt(owner, index++);
            boolean seen = false;
            for (int j = 0; j < index - 1 && !seen; j++) seen = snapshot.vertexLabelCodeAt(owner, j) == code;
            if (!seen) out.addValue(dictionary.get(code), in.bulk[i]);
        }
        return true;
    }

    @Override
    protected void doReset() {
        super.doReset();
        index = 0;
        count = 0;
    }

    @Override
    protected void onClose() {
        snapshot = null;
        dictionary = null;
    }
}
