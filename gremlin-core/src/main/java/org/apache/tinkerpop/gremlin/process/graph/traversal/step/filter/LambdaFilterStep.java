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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LambdaFilterStep<S> extends FilterStep<S> {

    private final Predicate<Traverser<S>> predicate;

    public LambdaFilterStep(final Traversal.Admin traversal, final Predicate<Traverser<S>> predicate) {
        super(traversal);
        this.predicate = predicate;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        return this.predicate.test(traverser);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this,this.predicate);
    }
}
