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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.util.Host;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LP_O_OB_P_S_SE_SL_Traverser<T> extends O_OB_S_SE_SL_Traverser<T> {

    protected Path path;

    protected LP_O_OB_P_S_SE_SL_Traverser() {
    }

    public LP_O_OB_P_S_SE_SL_Traverser(final T t, final Step<T, ?> step) {
        super(t, step);
        this.path = ImmutablePath.make().extend(t, step.getLabels());
    }

    /////////////////

    @Override
    public Path path() {
        return this.path;
    }

    /////////////////

    @Override
    public Traverser.Admin<T> detach() {
        super.detach();
        this.path = ReferenceFactory.detach(this.path);
        return this;
    }

    @Override
    public Traverser.Admin<T> attach(final Host host) {
        super.attach(host);
        if (host instanceof Partition || host instanceof Graph)
            this.path = host.attach(this.path);
        return this;
    }

    /////////////////

    @Override
    public <R> Traverser.Admin<R> split(final R r, final Step<T, R> step) {
        final LP_O_OB_P_S_SE_SL_Traverser<R> clone = (LP_O_OB_P_S_SE_SL_Traverser<R>) super.split(r, step);
        clone.path = clone.path.clone().extend(r, step.getLabels());
        return clone;
    }

    @Override
    public Traverser.Admin<T> split() {
        final LP_O_OB_P_S_SE_SL_Traverser<T> clone = (LP_O_OB_P_S_SE_SL_Traverser<T>) super.split();
        clone.path = clone.path.clone();
        return clone;
    }

    @Override
    public void addLabels(final Set<String> labels) {
        if (!labels.isEmpty())
            this.path = this.path.extend(labels);
    }

    @Override
    public void keepLabels(final Set<String> labels) {
        final Set<String> retractLabels = new HashSet<>();
        for (final Set<String> stepLabels : this.path.labels()) {
            for (final String l : stepLabels) {
                if (!labels.contains(l))
                    retractLabels.add(l);
            }
        }
        this.path = this.path.retract(retractLabels);
    }

    @Override
    public void dropLabels(final Set<String> labels) {
        if (!labels.isEmpty())
            this.path = path.retract(labels);
    }

    @Override
    public void dropPath() {
        this.path = ImmutablePath.make();
    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.path.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return (object instanceof LP_O_OB_P_S_SE_SL_Traverser)
                && ((LP_O_OB_P_S_SE_SL_Traverser) object).t.equals(this.t)
                && ((LP_O_OB_P_S_SE_SL_Traverser) object).future.equals(this.future)
                && ((LP_O_OB_P_S_SE_SL_Traverser) object).loops == this.loops
                && (null == this.sack || null != this.sideEffects.getSackMerger())
                && ((LP_O_OB_P_S_SE_SL_Traverser) object).path.equals(this.path);
    }

}
