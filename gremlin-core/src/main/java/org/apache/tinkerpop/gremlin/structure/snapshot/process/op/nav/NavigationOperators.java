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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.DenseFrontier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OpEstimate;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorRegistrar;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Sources;

/**
 * The operators for Scan, Lookup, MidScan, Expand, Endpoint, OtherV, Merge, Range and Dedup, with their estimators.
 */
public final class NavigationOperators implements OperatorRegistrar {

    @Override
    public void register(final CsrOperatorFactory factory) {
        factory.register(Sources.Scan.class, ScanOperator::new);
        factory.register(Sources.Lookup.class, LookupOperator::new);
        factory.register(Sources.MidScan.class, MidScanOperator::new);
        factory.register(Ops.Expand.class, ExpandOperator::new);
        factory.register(Ops.Endpoint.class, EndpointOperator::new);
        factory.register(Ops.OtherV.class, (node, spec) -> new OtherVOperator(spec));
        factory.register(Ops.Merge.class, (node, spec) -> new MergeOperator(spec));
        factory.register(Ops.Range.class, RangeOperator::new);
        factory.register(Ops.Dedup.class, DedupOperator::new);

        factory.registerEstimator(Sources.Scan.class, (node, snapshot, in) ->
                new OpEstimate(count(snapshot, node.lane()), in.memoryBytes()));
        factory.registerEstimator(Sources.Lookup.class, (node, snapshot, in) ->
                new OpEstimate(node.ids().size(), in.memoryBytes() + 4L * node.ids().size()));
        factory.registerEstimator(Sources.MidScan.class, (node, snapshot, in) ->
                new OpEstimate(count(snapshot, node.lane()), in.memoryBytes()));
        factory.registerEstimator(Ops.Expand.class, (node, snapshot, in) -> {
            final double perVertex = snapshot.vertexCount() == 0 ? 0
                    : (double) snapshot.edgeCount() / snapshot.vertexCount();
            double factor = perVertex * (node.direction() == Direction.BOTH ? 2 : 1);
            if (node.labelCodes() != null) factor *= labelFraction(snapshot, node.labelCodes());
            return in.withEntries(saturate(in.entries() * factor));
        });
        factory.registerEstimator(Ops.Endpoint.class, (node, snapshot, in) ->
                in.withEntries(node.direction() == Direction.BOTH ? saturate(in.entries() * 2.0) : in.entries()));
        factory.registerEstimator(Ops.Merge.class, (node, snapshot, in) -> {
            final long universe = Math.max(snapshot.vertexCount(), snapshot.edgeCount());
            final long sparse = 20L * in.entries();
            final long dense = DenseFrontier.bytesFor((int) Math.min(Integer.MAX_VALUE, universe));
            return new OpEstimate(Math.min(in.entries(), universe), in.memoryBytes() + Math.min(sparse, dense));
        });
        factory.registerEstimator(Ops.Range.class, (node, snapshot, in) ->
                in.withEntries(node.hi() < 0 ? in.entries() : Math.min(in.entries(), node.hi())));
        factory.registerEstimator(Ops.Dedup.class, (node, snapshot, in) -> {
            final long universe = Math.max(snapshot.vertexCount(), snapshot.edgeCount());
            final boolean bits = node.key() instanceof Keys.Identity || node.key() instanceof Keys.Token;
            return new OpEstimate(Math.min(in.entries(), universe),
                    in.memoryBytes() + (bits ? universe / 8 + 8 : 96L * in.entries()));
        });
    }

    private static long count(final CsrSnapshot snapshot, final Lane lane) {
        return lane == Lane.V ? snapshot.vertexCount() : snapshot.edgeCount();
    }

    private static long saturate(final double value) {
        return value >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) Math.ceil(value);
    }

    private static double labelFraction(final CsrSnapshot snapshot, final int[] codes) {
        if (snapshot.edgeCount() == 0) return 0;
        try {
            final long[] counts = snapshot.edgeLabelCounts();
            long sum = 0;
            for (final int code : codes) {
                if (code >= 0 && code < counts.length) sum += counts[code];
            }
            return Math.min(1.0, (double) sum / snapshot.edgeCount());
        } catch (final UnsupportedOperationException e) {
            return 1.0;
        }
    }
}
