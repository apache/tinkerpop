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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.filter;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OpEstimate;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorRegistrar;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Preds;

/**
 * The operator for Filter: the predicate compiler and the operator that applies it.
 */
public final class FilterOperators implements OperatorRegistrar {

    @Override
    public void register(final CsrOperatorFactory factory) {
        factory.register(Ops.Filter.class, FilterOperator::new);
        factory.registerEstimator(Ops.Filter.class, (node, snapshot, in) -> {
            if (node.pred() instanceof Preds.LaneType) return in;
            if (node.pred() instanceof Preds.IdIn ids) return in.withEntries(Math.min(in.entries(), ids.ordinals().length));
            return in.withEntries((in.entries() + 1) / 2);
        });
    }
}
