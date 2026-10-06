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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Sources;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Terminals;

import java.util.List;

/**
 * Registers a placeholder operator for every IR node class. The placeholder can be created but throws
 * {@link UnsupportedOperationException} when it is opened, so a plan that reaches an unimplemented node fails at bind
 * time, before any result is produced. Real registrars registered later replace the placeholders.
 */
public final class StubOperators implements OperatorRegistrar {

    /**
     * Every concrete node class of the IR.
     */
    public static final List<Class<? extends CsrOp>> NODE_CLASSES = List.of(
            Sources.Scan.class, Sources.Lookup.class, Sources.MidScan.class, Sources.Input.class,
            Ops.Expand.class, Ops.Endpoint.class, Ops.OtherV.class, Ops.Filter.class, Ops.Exists.class,
            Ops.NotExists.class, Ops.And.class, Ops.Or.class, Ops.Merge.class, Ops.Range.class, Ops.Dedup.class,
            Ops.Props.class, Ops.PropKey.class, Ops.PropValue.class, Ops.Id.class, Ops.Label.class, Ops.Labels.class,
            Ops.Element.class, Ops.Constant.class, Ops.Local.class, Ops.Union.class, Ops.Coalesce.class,
            Ops.Optional.class, Ops.Choose.class, Ops.MapFirst.class, Ops.FlatMap.class, Ops.Repeat.class,
            Ops.Loops.class, Ops.AggregateSideEffect.class, Ops.GroupCountSideEffect.class, Ops.GroupSideEffect.class,
            Terminals.Count.class, Terminals.Sum.class, Terminals.Min.class, Terminals.Max.class, Terminals.Mean.class,
            Terminals.GroupCount.class, Terminals.Group.class, Terminals.TopK.class, Terminals.Sort.class,
            Terminals.Fold.class, Terminals.Drain.class);

    @Override
    public void register(final CsrOperatorFactory factory) {
        for (final Class<? extends CsrOp> nodeClass : NODE_CLASSES) registerStub(factory, nodeClass);
    }

    private static <N extends CsrOp> void registerStub(final CsrOperatorFactory factory, final Class<N> nodeClass) {
        factory.registerStub(nodeClass, (node, spec) -> new Unsupported(spec));
    }

    private static final class Unsupported extends AbstractCsrOperator {

        Unsupported(final OperatorSpec spec) {
            super(spec);
        }

        private UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("The native operator for " + spec.node().name()
                    + " is not implemented");
        }

        @Override
        protected void doOpen() {
            throw unsupported();
        }

        @Override
        protected boolean produce(final Batch out) {
            throw unsupported();
        }

        @Override
        protected void doReset() {
        }

        @Override
        protected void doClose() {
        }
    }
}
