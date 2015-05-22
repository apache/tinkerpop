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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Randall Barnhart (random pi)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TimeLimitStep<S> extends FilterStep<S> {

    private AtomicLong startTime = new AtomicLong(-1);
    private final long timeLimit;
    private AtomicBoolean timedOut = new AtomicBoolean(false);


    public TimeLimitStep(final Traversal.Admin traversal, final long timeLimit) {
        super(traversal);
        this.timeLimit = timeLimit;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        if (this.startTime.get() == -1l)
            this.startTime.set(System.currentTimeMillis());
        if ((System.currentTimeMillis() - this.startTime.get()) >= this.timeLimit) {
            this.timedOut.set(true);
            throw FastNoSuchElementException.instance();
        }
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.timeLimit);
    }

    @Override
    public void reset() {
        super.reset();
        this.startTime.set(-1l);
        this.timedOut.set(false);
    }

    public boolean getTimedOut() {
        return this.timedOut.get();
    }

    @Override
    public TimeLimitStep<S> clone() {
        final TimeLimitStep<S> clone = (TimeLimitStep<S>) super.clone();
        clone.timedOut = new AtomicBoolean(this.timedOut.get());
        clone.startTime = new AtomicLong(this.startTime.get());
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Long.hashCode(this.timeLimit);
    }
}
