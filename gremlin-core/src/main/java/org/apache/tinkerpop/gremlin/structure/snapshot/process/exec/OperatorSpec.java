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

/**
 * Everything the factory knows about a node when it creates its operator, derived from the plan.
 *
 * @param node                 the IR node
 * @param index                the position of the node in its plan
 * @param inputLane            the lane of the upstream batches, null for a source
 * @param inputRecordsSource   whether the upstream {@code E} batches carry the source vertex
 * @param outputLane           the lane of the batches this operator emits
 * @param outputRecordsSource  whether the emitted {@code E} batches carry the source vertex
 * @param upstream             the operator to pull from, null for a source
 * @param input                the supplier for an {@code Input} source, null otherwise
 */
public record OperatorSpec(CsrOp node, int index, Lane inputLane, boolean inputRecordsSource, Lane outputLane,
                           boolean outputRecordsSource, CsrOperator upstream, BatchSupplier input) {

    /**
     * A batch in the shape this operator emits.
     */
    public Batch newOutputBatch(final int capacity) {
        return new Batch(outputLane, capacity, outputRecordsSource);
    }

    /**
     * A batch in the shape of the upstream operator's output.
     */
    public Batch newInputBatch(final int capacity) {
        if (inputLane == null) throw new IllegalStateException(node.name() + " has no input");
        return new Batch(inputLane, capacity, inputRecordsSource);
    }
}
