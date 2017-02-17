/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasPropertyStep<S extends Element> extends FilterStep<S> {

    private final String propertyKey;
    private final boolean contains;

    public HasPropertyStep(final Traversal.Admin traversal, final String propertyKey, final boolean contains) {
        super(traversal);
        this.propertyKey = propertyKey;
        this.contains = contains;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        return this.contains == traverser.get().properties(this.propertyKey).hasNext();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.contains ? this.propertyKey : "!" + this.propertyKey);
    }

    @Override
    public int hashCode() {
        return this.propertyKey.hashCode() ^ Boolean.hashCode(this.contains);
    }
}
