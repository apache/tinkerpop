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

import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnCursor;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav.EntryStreamOperator;

import java.util.Arrays;

/**
 * The operators that emit exactly one entry for each input entry: {@code id()}, {@code label()}, {@code key()},
 * {@code value()}, {@code element()} and {@code constant()}. The bulk of the entry is kept.
 */
final class SimpleOperators {

    private SimpleOperators() {
    }

    /**
     * {@code id()} of a vertex, an edge or a vertex property.
     */
    static final class IdOperator extends EntryStreamOperator {
        private CsrSnapshot snapshot;

        IdOperator(final OperatorSpec spec) {
            super(spec);
        }

        @Override
        protected void onOpen() {
            snapshot = ctx.snapshot();
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            final Object id;
            switch (in.lane) {
                case V:
                    id = snapshot.vertexId(in.ord[i]);
                    break;
                case E:
                    id = snapshot.edgeId(in.ord[i]);
                    break;
                default:
                    id = snapshot.vertexPropertyIdentifier(in.key[i], in.aux[i]);
                    break;
            }
            out.addValue(id, in.bulk[i]);
            return true;
        }

        @Override
        protected void onClose() {
            snapshot = null;
        }
    }

    /**
     * {@code label()}: the first label of a vertex, or the empty string, and the label of an edge.
     */
    static final class LabelOperator extends EntryStreamOperator {
        private CsrSnapshot snapshot;

        LabelOperator(final OperatorSpec spec) {
            super(spec);
        }

        @Override
        protected void onOpen() {
            snapshot = ctx.snapshot();
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            out.addValue(in.lane == Lane.V ? snapshot.vertexLabel(in.ord[i]) : snapshot.edgeLabel(in.ord[i]),
                    in.bulk[i]);
            return true;
        }

        @Override
        protected void onClose() {
            snapshot = null;
        }
    }

    /**
     * {@code key()} of a vertex property, an edge property or a meta-property.
     */
    static final class PropKeyOperator extends EntryStreamOperator {
        private CsrGraph graph;

        PropKeyOperator(final OperatorSpec spec) {
            super(spec);
        }

        @Override
        protected void onOpen() {
            graph = ctx.graph();
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            final String key;
            switch (in.lane) {
                case VP:
                    key = graph.vertexKey(in.key[i]);
                    break;
                case EP:
                    key = graph.edgeKey(in.key[i]);
                    break;
                default:
                    key = graph.metaKey(in.src[i]);
                    break;
            }
            out.addValue(key, in.bulk[i]);
            return true;
        }

        @Override
        protected void onClose() {
            graph = null;
        }
    }

    /**
     * {@code value()}: a lazy reference to the value column entry for vertex and edge properties, the decoded value for
     * meta-properties. A null value is emitted as null.
     */
    static final class PropValueOperator extends EntryStreamOperator {
        private CsrGraph graph;
        private CsrSnapshot snapshot;
        private int[] columnIds;
        private ColumnCursor[] cursors;

        PropValueOperator(final OperatorSpec spec) {
            super(spec);
        }

        @Override
        protected void onOpen() {
            graph = ctx.graph();
            snapshot = ctx.snapshot();
            if (in() == Lane.VP) {
                columnIds = new int[graph.vertexKeyCount()];
            } else if (in() == Lane.EP) {
                columnIds = new int[graph.edgeKeyCount()];
                cursors = new ColumnCursor[columnIds.length];
            }
            if (columnIds != null) Arrays.fill(columnIds, -1);
        }

        private Lane in() {
            return spec.inputLane();
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            final int key = in.key[i];
            switch (in.lane) {
                case VP:
                    if (columnIds[key] < 0) columnIds[key] = ctx.registerColumn(snapshot.vertexPropertyColumn(key));
                    out.addColumnValue(columnIds[key], in.aux[i], in.bulk[i]);
                    break;
                case EP:
                    if (columnIds[key] < 0) {
                        columnIds[key] = ctx.registerColumn(graph.edgeColumn(key));
                        cursors[key] = graph.edgeColumn(key).cursor();
                    }
                    out.addColumnValue(columnIds[key], cursors[key].seek(in.ord[i]), in.bulk[i]);
                    break;
                default:
                    out.addValue(snapshot.metaPropertyValue(key, in.src[i], in.aux[i]), in.bulk[i]);
                    break;
            }
            return true;
        }

        @Override
        protected void onClose() {
            graph = null;
            snapshot = null;
            columnIds = null;
            cursors = null;
        }
    }

    /**
     * {@code element()}: the owner of a property.
     */
    static final class ElementOperator extends EntryStreamOperator {

        ElementOperator(final OperatorSpec spec) {
            super(spec);
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            switch (in.lane) {
                case VP:
                    out.addV(in.ord[i], in.bulk[i]);
                    break;
                case EP:
                    out.addE(in.ord[i], in.bulk[i]);
                    break;
                default:
                    out.addVP(in.ord[i], in.key[i], in.aux[i], in.bulk[i]);
                    break;
            }
            return true;
        }
    }

    /**
     * {@code constant(v)}: the value, read when the operator opens so that a bound {@code GValue} is current.
     */
    static final class ConstantOperator extends EntryStreamOperator {
        private final Ops.Constant node;
        private Object value;

        ConstantOperator(final Ops.Constant node, final OperatorSpec spec) {
            super(spec);
            this.node = node;
        }

        @Override
        protected void onOpen() {
            bind();
        }

        @Override
        protected void onReset() {
            bind();
        }

        private void bind() {
            final Object raw = node.value();
            value = raw instanceof GValue ? ((GValue<?>) raw).get() : raw;
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            out.addValue(value, in.bulk[i]);
            return true;
        }
    }
}
