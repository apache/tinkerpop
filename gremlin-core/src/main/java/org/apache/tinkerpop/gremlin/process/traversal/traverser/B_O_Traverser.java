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
package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class B_O_Traverser<T> extends O_Traverser<T> {

    protected long bulk = 1l;
    protected String future = HALT;

    protected B_O_Traverser() {
    }

    public B_O_Traverser(final T t, final long initialBulk) {
        super(t);
        this.bulk = initialBulk;
    }

    @Override
    public void setBulk(final long count) {
        this.bulk = count;
    }

    @Override
    public long bulk() {
        return this.bulk;
    }

    @Override
    public void merge(final Traverser.Admin<?> other) {
        super.merge(other);
        this.bulk = this.bulk + other.bulk();
    }

    @Override
    public String getStepId() {
        return this.future;
    }

    @Override
    public void setStepId(final String stepId) {
        this.future = stepId;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    protected final boolean equals(final B_O_Traverser other) {
        return super.equals(other) && other.future.equals(this.future);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof B_O_Traverser && this.equals((B_O_Traverser) object);
    }
}
