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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.terminal;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorRegistrar;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Terminals;

/**
 * The operators for Count, Sum, Min, Max, Mean, GroupCount, Group, TopK, Sort, Fold and Drain. Count, sum, min, max,
 * mean and fold are {@link ReducerOperator}s over the {@link Reducers}; group count and group share the
 * {@link GroupAccumulator}; top-k and sort keep {@link OrderedRows} in the heap. The sort is in-heap, and the group
 * tables have no spill, until the spillable versions of the spill package replace these registrations.
 */
public final class TerminalOperators implements OperatorRegistrar {

    @Override
    public void register(final CsrOperatorFactory factory) {
        factory.register(Terminals.Count.class, (node, spec) -> new ReducerOperator(spec));
        factory.register(Terminals.Sum.class, (node, spec) -> new ReducerOperator(spec));
        factory.register(Terminals.Min.class, (node, spec) -> new ReducerOperator(spec));
        factory.register(Terminals.Max.class, (node, spec) -> new ReducerOperator(spec));
        factory.register(Terminals.Mean.class, (node, spec) -> new ReducerOperator(spec));
        factory.register(Terminals.Fold.class, (node, spec) -> new ReducerOperator(spec));
        factory.register(Terminals.GroupCount.class, (node, spec) -> new GroupTerminalOperator(spec));
        factory.register(Terminals.Group.class, (node, spec) -> new GroupTerminalOperator(spec));
        factory.register(Terminals.TopK.class, (node, spec) -> new TopKOperator(spec));
        factory.register(Terminals.Sort.class, (node, spec) -> new SortOperator(spec));
        factory.register(Terminals.Drain.class, (node, spec) -> new DrainOperator(spec));
    }
}
