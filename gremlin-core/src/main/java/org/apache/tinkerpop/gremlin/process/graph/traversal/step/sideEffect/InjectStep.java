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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.Traversal;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class InjectStep<S> extends StartStep<S> {

    private final List<S> injections;

    @SafeVarargs
    public InjectStep(final Traversal.Admin traversal, final S... injections) {
        super(traversal);
        this.injections = Arrays.asList(injections);
        this.start = this.injections.iterator();
    }

    @Override
    public InjectStep<S> clone() throws CloneNotSupportedException {
        final InjectStep<S> clone = (InjectStep<S>)super.clone();
        clone.start = this.injections.iterator();
        return clone;
    }
}
