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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

/**
 * The operators for AggregateSideEffect, GroupCountSideEffect and GroupSideEffect, which pass their input through and
 * write the side effect once when the input is exhausted.
 */
public final class SideEffectOperators implements OperatorRegistrar {

    @Override
    public void register(final CsrOperatorFactory factory) {
        factory.register(Ops.AggregateSideEffect.class, (node, spec) -> new AggregateWriterOperator(spec));
        factory.register(Ops.GroupCountSideEffect.class, (node, spec) -> new GroupWriterOperator(spec));
        factory.register(Ops.GroupSideEffect.class, (node, spec) -> new GroupWriterOperator(spec));
    }
}
