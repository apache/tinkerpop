/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class SampleLocalStep<S> extends LocalBarrierStep<S, S> {

    private final int amountToSample;

    public SampleLocalStep(final Traversal.Admin traversal, final int amountToSample) {
        super(traversal);
        this.amountToSample = amountToSample;
    }

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        final List elements = new ArrayList(this.collect(traverser));
        if (elements.size() <= this.amountToSample) return (S) elements;
        Collections.shuffle(elements);
        return (S) elements.subList(0, this.amountToSample);
    }
}
