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
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ClassFilterStep<S, T> extends FilterStep<S> {

    private final Class<T> classFilter;
    private final boolean isInstanceCheck;

    public ClassFilterStep(final Traversal.Admin traversal, final Class<T> classFilter, final boolean isInstanceCheck) {
        super(traversal);
        this.classFilter = classFilter;
        this.isInstanceCheck = isInstanceCheck;
    }

    public boolean filter(final Traverser.Admin<S> traverser) {
        return !(this.isInstanceCheck ?
                this.classFilter.isInstance(traverser.get()) :
                this.classFilter.equals(traverser.get().getClass()));

    }

    public int hashCode() {
        return super.hashCode() ^ this.classFilter.hashCode() + Boolean.valueOf(this.isInstanceCheck).hashCode();
    }

    public String toString() {
        return StringFactory.stepString(this, this.classFilter);
    }
}
