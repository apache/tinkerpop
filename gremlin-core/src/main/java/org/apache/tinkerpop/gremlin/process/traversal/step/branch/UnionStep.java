/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends BranchStep<S, E, TraversalOptionParent.Pick> {

    public UnionStep(final Traversal.Admin traversal, final Traversal.Admin<?, E>... unionTraversals) {
        super(traversal);
        this.setBranchTraversal(new ConstantTraversal<>(Pick.any));
        for (final Traversal.Admin<?, E> union : unionTraversals) {
            this.addGlobalChildOption(Pick.any, (Traversal.Admin) union);
        }
    }

    @Override
    public void addGlobalChildOption(final Pick pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (Pick.any != pickToken)
            throw new IllegalArgumentException("Union step only supports the any token: " + pickToken);
        super.addGlobalChildOption(pickToken, traversalOption);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.traversalPickOptions.getOrDefault(Pick.any, Collections.emptyList()));
    }
}
