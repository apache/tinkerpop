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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Sources;

/**
 * The operators that belong to the runtime core rather than to a work package: the {@code Input} source.
 */
public final class CoreOperators implements OperatorRegistrar {

    @Override
    public void register(final CsrOperatorFactory factory) {
        factory.register(Sources.Input.class, (node, spec) -> new InputOperator(spec));
    }

    /**
     * Emits the batches of the {@link BatchSupplier} the pipeline was built with.
     */
    private static final class InputOperator extends AbstractCsrOperator {

        private InputOperator(final OperatorSpec spec) {
            super(spec);
            if (spec.input() == null) throw new IllegalArgumentException("An Input source needs a BatchSupplier");
        }

        @Override
        protected void doOpen() {
        }

        @Override
        protected boolean produce(final Batch out) {
            return spec.input().next(out);
        }

        @Override
        protected void doReset() {
        }

        @Override
        protected void doClose() {
        }
    }
}
