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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.FromToModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathFilterStep<S> extends FilterStep<S> implements FromToModulating, ByModulating, TraversalParent, PathProcessor {

    protected String fromLabel;
    protected String toLabel;
    protected boolean isSimple;
    protected TraversalRing<Object, Object> traversalRing;
    protected Set<String> keepLabels;

    public PathFilterStep(final Traversal.Admin traversal, final boolean isSimple) {
        super(traversal);
        this.traversalRing = new TraversalRing<>();
        this.isSimple = isSimple;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        final Path path = traverser.path().subPath(this.fromLabel, this.toLabel);
        if (this.traversalRing.isEmpty())
            return path.isSimple() == this.isSimple;
        else {
            this.traversalRing.reset();
            final Path byPath = MutablePath.make();
            final List<Set<String>> labels = path.labels();
            final List<Object> objects = path.objects();
            for (int ix = 0; ix < objects.size(); ix++) {
                final Traversal.Admin t = traversalRing.next();
                final TraversalProduct p = TraversalUtil.produce(objects.get(ix), t);

                // if not productive we can quit coz this path is getting filtered
                if (!p.isProductive()) break;

                byPath.extend(p.get(), labels.get(ix));
            }

            // the path sizes must be equal or else it means a by() wasn't productive and that path will be filtered
            return path.size() == byPath.size() && byPath.isSimple() == this.isSimple;
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }

    public void addFrom(final String fromLabel) {
        this.fromLabel = fromLabel;
    }

    public void addTo(final String toLabel) {
        this.toLabel = toLabel;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.isSimple ? "simple" : "cyclic", this.fromLabel, this.toLabel, this.traversalRing);
    }

    @Override
    public PathFilterStep<S> clone() {
        final PathFilterStep<S> clone = (PathFilterStep<S>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> pathTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(pathTraversal));
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        this.traversalRing.replaceTraversal(
                (Traversal.Admin<Object, Object>) oldTraversal,
                (Traversal.Admin<Object, Object>) newTraversal);
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^
                this.traversalRing.hashCode() ^
                Boolean.hashCode(this.isSimple) ^
                (null == this.fromLabel ? "null".hashCode() : this.fromLabel.hashCode()) ^
                (null == this.toLabel ? "null".hashCode() : this.toLabel.hashCode());
    }

    @Override
    public void setKeepLabels(final Set<String> keepLabels) {
        this.keepLabels = new HashSet<>(keepLabels);
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        return PathProcessor.processTraverserPathLabels(super.processNextStart(), this.keepLabels);
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

    public String getFromLabel() {
        return fromLabel;
    }

    public String getToLabel() {
        return toLabel;
    }

    public boolean isSimple() {
        return isSimple;
    }

    public TraversalRing<Object, Object> getTraversalRing() {
        return traversalRing;
    }
}
