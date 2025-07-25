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

import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.HasNextStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A step which offers a choice of two or more Traversals to take.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseStep<S, E, M> extends BranchStep<S, E, M> {

    public ChooseStep(final Traversal.Admin traversal, final Traversal.Admin<S, M> choiceTraversal) {
        super(traversal);
        this.setBranchTraversal(choiceTraversal);
        final Traversal.Admin<S,E> identityPassthrough = new DefaultGraphTraversal<>();
        identityPassthrough.addStep(new IdentityStep<>(traversal));
        this.addChildOption((M) Pick.unproductive, identityPassthrough);
    }

    public ChooseStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?> predicateTraversal, final Traversal.Admin<S, E> trueChoice, final Traversal.Admin<S, E> falseChoice) {
        this(traversal, (Traversal.Admin<S, M>) predicateTraversal.addStep(new HasNextStep<>(predicateTraversal)));
        this.addChildOption((M) Boolean.TRUE, trueChoice);
        this.addChildOption((M) Boolean.FALSE, falseChoice);
    }

    @Override
    public void addChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (pickToken instanceof Pick) {
            if (Pick.any.equals(pickToken))
                throw new IllegalArgumentException("Choose step can not have an any-option as only one option per traverser is allowed");
            if (this.traversalPickOptions.containsKey(pickToken))
                throw new IllegalArgumentException("Choose step can only have one traversal per pick token: " + pickToken);
        }
        super.addChildOption(pickToken, traversalOption);
    }

    @Override
    protected List<Traversal.Admin<S, E>> pickBranches(final M choice) {
        final List<Traversal.Admin<S, E>> branches = super.pickBranches(choice);
        return branches != null ? branches.subList(0, 1) : null;
    }
}
