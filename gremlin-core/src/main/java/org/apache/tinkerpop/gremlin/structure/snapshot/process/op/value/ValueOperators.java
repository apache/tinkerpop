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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OpEstimate;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorRegistrar;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

/**
 * WP18: the operators that read properties, ids, labels and keys and that build maps, emitting plain values wherever
 * the standard step's output holds no element or property.
 * <ul>
 *     <li>{@code Props}, {@code PropKey}, {@code PropValue}, {@code Id}, {@code Label}, {@code Labels},
 *     {@code Element} and {@code Constant} from the shared IR;</li>
 *     <li>{@link MapOps.ValueMap}, {@link MapOps.PropertyMap}, {@link MapOps.ElementMap} and {@link MapOps.Project},
 *     nodes that live in this package.</li>
 * </ul>
 */
public final class ValueOperators implements OperatorRegistrar {

    @Override
    public void register(final CsrOperatorFactory factory) {
        factory.register(Ops.Props.class, PropsOperator::new);
        factory.register(Ops.PropKey.class, (node, spec) -> new SimpleOperators.PropKeyOperator(spec));
        factory.register(Ops.PropValue.class, (node, spec) -> new SimpleOperators.PropValueOperator(spec));
        factory.register(Ops.Id.class, (node, spec) -> new SimpleOperators.IdOperator(spec));
        factory.register(Ops.Label.class, (node, spec) -> new SimpleOperators.LabelOperator(spec));
        factory.register(Ops.Labels.class, (node, spec) -> new LabelsOperator(spec));
        factory.register(Ops.Element.class, (node, spec) -> new SimpleOperators.ElementOperator(spec));
        factory.register(Ops.Constant.class, SimpleOperators.ConstantOperator::new);
        factory.register(MapOps.ValueMap.class, MapOperators.ValueMapOperator::new);
        factory.register(MapOps.PropertyMap.class, MapOperators.PropertyMapOperator::new);
        factory.register(MapOps.ElementMap.class, MapOperators.ElementMapOperator::new);
        factory.register(MapOps.Project.class, MapOperators.ProjectOperator::new);

        factory.registerEstimator(Ops.Props.class, (node, snapshot, in) -> {
            final double factor = node.keyCodes().length == 0 ? allKeys(snapshot) : node.keyCodes().length;
            return in.withEntries(saturate(in.entries() * factor));
        });
        factory.registerEstimator(Ops.Labels.class, (node, snapshot, in) -> {
            if (!snapshot.isMultiLabel() || snapshot.vertexCount() == 0) return in;
            long labels = 0;
            for (final long count : snapshot.vertexLabelCounts()) labels += count;
            return in.withEntries(saturate(in.entries() * Math.max(1.0, (double) labels / snapshot.vertexCount())));
        });
    }

    // the most properties per vertex or edge when every key is read, on average
    private static double allKeys(final CsrSnapshot snapshot) {
        double vertices = 1;
        double edges = 1;
        try {
            long present = 0;
            for (int k = 0; k < snapshot.vertexPropertyKeys().size(); k++) {
                present += snapshot.vertexPropertyColumn(k).presentCount();
            }
            if (snapshot.vertexCount() > 0) vertices = (double) present / snapshot.vertexCount();
            present = 0;
            for (int k = 0; k < snapshot.edgePropertyKeys().size(); k++) {
                present += snapshot.edgePropertyColumn(k).presentCount();
            }
            if (snapshot.edgeCount() > 0) edges = (double) present / snapshot.edgeCount();
        } catch (UnsupportedOperationException e) {
            // no property columns in this layout
        }
        return Math.max(1.0, Math.max(vertices, edges));
    }

    private static long saturate(final double value) {
        return value >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) Math.ceil(value);
    }
}
