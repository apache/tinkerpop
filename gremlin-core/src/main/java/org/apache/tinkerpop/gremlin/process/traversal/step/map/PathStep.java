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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.FromToModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathStep<S> extends MapStep<S, Path> implements TraversalParent, PathProcessor, ByModulating, FromToModulating {

    private TraversalRing<Object, Object> traversalRing;
    private Set<String> keepLabels;
    private String fromLabel;
    private String toLabel;

    public PathStep(final Traversal.Admin traversal) {
        super(traversal);
        this.traversalRing = new TraversalRing<>();
    }

    @Override
    public PathStep<S> clone() {
        final PathStep<S> clone = (PathStep<S>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.traversalRing.hashCode();
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
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
    public String toString() {
        return StringFactory.stepString(this, this.traversalRing);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH);
    }

    @Override
    public void setKeepLabels(final Set<String> keepLabels) {
        this.keepLabels = new HashSet<>(keepLabels);
    }

    @Override
    protected Traverser.Admin<Path> processNextStart() {
        final Traverser.Admin<S> traverser = this.starts.next();
        final Path path = traverser.path().subPath(this.fromLabel, this.toLabel);
        if (this.traversalRing.isEmpty())
            return PathProcessor.processTraverserPathLabels(traverser.split(path, this), this.keepLabels);
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
            return path.size() == byPath.size() ?
                    PathProcessor.processTraverserPathLabels(traverser.split(byPath, this), this.keepLabels) :
                    EmptyTraverser.instance();
        }
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }


    @Override
    public void addFrom(final String fromLabel) {
        this.fromLabel = fromLabel;
    }

    @Override
    public void addTo(final String toLabel) {
        this.toLabel = toLabel;
    }

    public String getFromLabel() {
        return fromLabel;
    }

    public String getToLabel() {
        return toLabel;
    }
}
