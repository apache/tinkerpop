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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Materializer;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav.EntryStreamOperator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.util.NumberHelper.max;

/**
 * The operators behind {@link MapOps}. Each builds one {@code LinkedHashMap} per input entry from the owner ranges and
 * columns, in the key order of the standard step, and emits it as a decoded {@code VAL} entry with the entry's bulk.
 */
final class MapOperators {

    private MapOperators() {
    }

    /**
     * The state the three element maps share: the resolved keys and the lane.
     */
    private abstract static class ElementMapper extends EntryStreamOperator {
        private final int[] requested;
        protected final boolean vertices;
        protected final boolean multiLabel;
        protected CsrGraph graph;
        protected CsrSnapshot snapshot;
        protected KeyRanges keys;

        ElementMapper(final OperatorSpec spec, final int[] requested, final boolean multiLabel) {
            super(spec);
            this.requested = requested;
            this.vertices = spec.inputLane() == Lane.V;
            this.multiLabel = multiLabel;
        }

        @Override
        protected void onOpen() {
            graph = ctx.graph();
            snapshot = ctx.snapshot();
            keys = new KeyRanges(graph, vertices, requested);
        }

        @Override
        protected void onClose() {
            graph = null;
            snapshot = null;
            keys = null;
        }

        /**
         * {@code T.label} as the standard steps put it: the label set when multi-label, else the label unless empty.
         */
        protected void putLabel(final Map<Object, Object> map, final boolean vertex, final int ordinal) {
            if (multiLabel) {
                map.put(T.label, vertex ? KeyRanges.vertexLabels(snapshot, ordinal)
                        : java.util.Collections.singleton(snapshot.edgeLabel(ordinal)));
            } else {
                final String label = vertex ? snapshot.vertexLabel(ordinal) : snapshot.edgeLabel(ordinal);
                if (!label.isEmpty()) map.put(T.label, label);
            }
        }
    }

    /**
     * {@code valueMap(k...)}.
     */
    static final class ValueMapOperator extends ElementMapper {
        private final int tokens;

        ValueMapOperator(final MapOps.ValueMap node, final OperatorSpec spec) {
            super(spec, node.keyCodes(), node.multiLabel());
            this.tokens = node.tokens();
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean process(final Batch in, final int i, final Batch out) {
            final int ordinal = in.ord[i];
            final Map<Object, Object> map = new LinkedHashMap<>();
            if ((tokens & WithOptions.ids) != 0) map.put(T.id, vertices ? snapshot.vertexId(ordinal) : snapshot.edgeId(ordinal));
            if ((tokens & WithOptions.labels) != 0) putLabel(map, vertices, ordinal);
            for (int k = 0; k < keys.size(); k++) {
                if (!keys.seek(k, ordinal)) continue;
                final String name = keys.name(k);
                if (vertices) {
                    List<Object> values = (List<Object>) map.get(name);
                    if (values == null) {
                        values = new ArrayList<>();
                        map.put(name, values);
                    }
                    for (long p = keys.start(); p < keys.end(); p++) {
                        values.add(snapshot.vertexPropertyValue(keys.code(k), p));
                    }
                } else {
                    map.put(name, keys.column(k).getAt(keys.start()));
                }
            }
            out.addValue(map, in.bulk[i]);
            return true;
        }
    }

    /**
     * {@code propertyMap(k...)}: the values are property facades.
     */
    static final class PropertyMapOperator extends ElementMapper {

        PropertyMapOperator(final MapOps.PropertyMap node, final OperatorSpec spec) {
            super(spec, node.keyCodes(), false);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean process(final Batch in, final int i, final Batch out) {
            final int ordinal = in.ord[i];
            final Map<Object, Object> map = new LinkedHashMap<>();
            for (int k = 0; k < keys.size(); k++) {
                if (!keys.seek(k, ordinal)) continue;
                final String name = keys.name(k);
                if (vertices) {
                    List<Object> values = (List<Object>) map.get(name);
                    if (values == null) {
                        values = new ArrayList<>();
                        map.put(name, values);
                    }
                    for (long p = keys.start(); p < keys.end(); p++) {
                        values.add(graph.vertexPropertyAt(ordinal, keys.code(k), p));
                    }
                } else {
                    map.put(name, graph.edgePropertyAt(ordinal, keys.code(k)));
                }
            }
            out.addValue(map, in.bulk[i]);
            return true;
        }
    }

    /**
     * {@code elementMap(k...)}.
     */
    static final class ElementMapOperator extends ElementMapper {

        ElementMapOperator(final MapOps.ElementMap node, final OperatorSpec spec) {
            super(spec, node.keyCodes(), node.multiLabel());
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            final int ordinal = in.ord[i];
            final Map<Object, Object> map = new LinkedHashMap<>();
            map.put(T.id, vertices ? snapshot.vertexId(ordinal) : snapshot.edgeId(ordinal));
            putLabel(map, vertices, ordinal);
            if (!vertices) {
                map.put(Direction.IN, structure(snapshot.edgeIn(ordinal)));
                map.put(Direction.OUT, structure(snapshot.edgeOut(ordinal)));
            }
            for (int k = 0; k < keys.size(); k++) {
                if (!keys.seek(k, ordinal)) continue;
                // the last value of a multi-property wins, and the key keeps the position of its first put
                final Object value = vertices ? snapshot.vertexPropertyValue(keys.code(k), keys.end() - 1)
                        : keys.column(k).getAt(keys.start());
                map.put(keys.name(k), value);
            }
            out.addValue(map, in.bulk[i]);
            return true;
        }

        private Map<Object, Object> structure(final int vertex) {
            final Map<Object, Object> m = new LinkedHashMap<>();
            m.put(T.id, snapshot.vertexId(vertex));
            putLabel(m, true, vertex);
            return m;
        }
    }

    /**
     * {@code project(names...).by(keys...)}.
     */
    static final class ProjectOperator extends EntryStreamOperator {
        private final MapOps.Project node;
        private ProjectKeyReader[] readers;

        ProjectOperator(final MapOps.Project node, final OperatorSpec spec) {
            super(spec);
            this.node = node;
        }

        @Override
        protected void onOpen() {
            final List<Keys.Key> keys = node.keys();
            readers = new ProjectKeyReader[keys.size()];
            try {
                for (int k = 0; k < readers.length; k++) {
                    readers[k] = new ProjectKeyReader(ctx, keys.get(k), spec.inputLane());
                }
            } catch (RuntimeException e) {
                closeReaders();
                throw e;
            }
        }

        @Override
        protected void onReset() {
            for (final ProjectKeyReader reader : readers) reader.reset();
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            final List<String> names = node.names();
            final Map<String, Object> map = new LinkedHashMap<>(names.size(), 1.0f);
            for (int k = 0; k < readers.length; k++) {
                final Object value = readers[k].read(in, i);
                if (value != ProjectKeyReader.UNPRODUCTIVE) map.put(names.get(k), value);
            }
            out.addValue(map, in.bulk[i]);
            return true;
        }

        @Override
        protected void onClose() {
            closeReaders();
        }

        private void closeReaders() {
            if (readers == null) return;
            for (final ProjectKeyReader reader : readers) {
                if (reader != null) reader.close();
            }
            readers = null;
        }
    }

    static final class SelectColumnOperator extends EntryStreamOperator {
        private final MapOps.SelectColumn node;

        SelectColumnOperator(final MapOps.SelectColumn node, final OperatorSpec spec) {
            super(spec);
            this.node = node;
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            out.addValue(node.column().apply(Materializer.value(ctx, in, i)), in.bulk[i]);
            return true;
        }
    }

    static final class CountLocalOperator extends EntryStreamOperator {

        CountLocalOperator(final OperatorSpec spec) {
            super(spec);
        }

        @Override
        protected boolean process(final Batch in, final int i, final Batch out) {
            final Object item = Materializer.materialize(ctx, in, i);
            final long count = item instanceof Tree ? ((Tree) item).nodeCount()
                    : item instanceof Collection ? ((Collection<?>) item).size()
                    : item instanceof Map ? ((Map<?, ?>) item).size()
                    : item instanceof Path ? ((Path) item).size()
                    : IteratorUtils.count(IteratorUtils.asIterator(item));
            out.addValue(count, in.bulk[i]);
            return true;
        }
    }

    static final class MaxLocalOperator extends EntryStreamOperator {

        MaxLocalOperator(final OperatorSpec spec) {
            super(spec);
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        protected boolean process(final Batch in, final int i, final Batch out) {
            final Iterator<?> iterator = IteratorUtils.asIterator(Materializer.materialize(ctx, in, i));
            if (!iterator.hasNext()) return true;
            Comparable result = (Comparable) iterator.next();
            while (iterator.hasNext()) result = max((Comparable) iterator.next(), result);
            out.addValue(result, in.bulk[i]);
            return true;
        }
    }
}
