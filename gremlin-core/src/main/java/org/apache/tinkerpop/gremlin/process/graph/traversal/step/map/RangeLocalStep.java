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
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.LocalBarrierStep;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class RangeLocalStep<S> extends LocalBarrierStep<S, S> {

    private final long low;
    private final long high;

    public RangeLocalStep(final Traversal.Admin traversal, final long low, final long high) {
        super(traversal);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + ']');
        }
        this.low = low;
        this.high = high;
    }

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        Stream stream = this.collect(traverser).stream().skip(this.low);
        if (this.high != -1) {
            stream = stream.limit(this.high - this.low);
        }
        return (S) stream.collect(Collectors.toList());
    }
}
