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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorRegistrar;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Terminals;

/**
 * WP21: the spillable replacements for the stateful operators, registered last so they override the in-memory ones.
 * {@code Dedup}, {@code GroupCount}, {@code Group} and {@code Sort} keep their state within the memory budget of the
 * execution and write what does not fit to scratch segments, see {@link SpillDedupOperator},
 * {@link SpillGroupCountOperator}, {@link SpillGroupOperator} and {@link SpillSortOperator}. The side-effect writers
 * {@code groupCount('x')} and {@code group('x')} reuse the group states, see {@link SpillGroupCountWriterOperator} and
 * {@link SpillGroupWriterOperator}. {@link SpillFrontier} is the budget-aware frontier for operators that iterate over
 * levels.
 */
public final class SpillOperators implements OperatorRegistrar {

    @Override
    public void register(final CsrOperatorFactory factory) {
        factory.register(Ops.Dedup.class, SpillDedupOperator::new);
        factory.register(Terminals.GroupCount.class, SpillGroupCountOperator::new);
        factory.register(Terminals.Group.class, SpillGroupOperator::new);
        factory.register(Terminals.Sort.class, SpillSortOperator::new);
        factory.register(Ops.GroupCountSideEffect.class, SpillGroupCountWriterOperator::new);
        factory.register(Ops.GroupSideEffect.class, SpillGroupWriterOperator::new);
    }
}
