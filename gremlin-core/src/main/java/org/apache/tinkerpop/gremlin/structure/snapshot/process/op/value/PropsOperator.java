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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

/**
 * {@code values(k...)} and {@code properties(k...)}: for each vertex or edge, the properties of each requested key in
 * argument order, or of every key in key-code order when none were given; the properties of one key come in source
 * order. {@code VALUE} mode emits lazy references to the value column entries (so a present null is emitted as null) and
 * {@code PROPERTY} mode emits {@code VP} or {@code EP} entries. Nothing is decoded or created here.
 */
final class PropsOperator extends FanOutOperator {

    private final Ops.Props node;
    private final boolean vertices;
    private final boolean values;
    private KeyRanges keys;
    private int[] columnIds;

    // the position inside the current input entry
    private int key;
    private long position;
    private long end;

    PropsOperator(final Ops.Props node, final OperatorSpec spec) {
        super(spec);
        this.node = node;
        this.vertices = spec.inputLane() == Lane.V;
        this.values = node.mode() == Ops.PropMode.VALUE;
    }

    @Override
    protected void onOpen() {
        keys = new KeyRanges(ctx.graph(), vertices, node.keyCodes());
        columnIds = new int[keys.size()];
        if (values) {
            for (int k = 0; k < columnIds.length; k++) columnIds[k] = ctx.registerColumn(keys.column(k));
        }
    }

    @Override
    protected void begin(final Batch in, final int i) {
        key = 0;
        position = 0;
        end = 0;
    }

    @Override
    protected boolean emit(final Batch in, final int i, final Batch out) {
        final int owner = in.ord[i];
        final long bulk = in.bulk[i];
        while (true) {
            while (position >= end) {
                if (key >= keys.size()) return true;
                if (keys.seek(key, owner)) {
                    position = keys.start();
                    end = keys.end();
                } else {
                    position = 0;
                    end = 0;
                }
                key++;
            }
            final int k = key - 1;
            while (position < end) {
                if (out.isFull()) return false;
                if (values) out.addColumnValue(columnIds[k], position, bulk);
                else if (vertices) out.addVP(owner, keys.code(k), position, bulk);
                else out.addEP(owner, keys.code(k), bulk);
                position++;
            }
            ctx.checkInterrupt();
        }
    }

    @Override
    protected void doReset() {
        super.doReset();
        key = 0;
        position = 0;
        end = 0;
    }

    @Override
    protected void onClose() {
        keys = null;
        columnIds = null;
    }
}
