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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class ConstantStep<S, E> extends ScalarMapStep<S, E> {

    private final GValue<E> constant;

    public ConstantStep(final Traversal.Admin traversal, final E constant) {
        this(traversal, constant instanceof GValue ? (GValue) constant : GValue.of(null, constant));
    }

    public ConstantStep(final Traversal.Admin traversal, final GValue<E> constant) {
        super(traversal);
        this.constant = null == constant ? GValue.of(null, null) : constant;
    }

    public E getConstant() {
        return this.constant.get();
    }
    public GValue<E> getConstantGValue() {
        return this.constant;
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        return this.constant.get();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.constant.get());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.constant.get());
    }
}
