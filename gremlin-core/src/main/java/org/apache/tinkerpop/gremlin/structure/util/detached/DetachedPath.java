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
package org.apache.tinkerpop.gremlin.structure.util.detached;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.util.LinkedHashSet;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedPath extends MutablePath implements Attachable<Path> {

    private DetachedPath() {

    }

    public Path get() {
        return this;
    }

    protected DetachedPath(final Path path, final boolean withProperties) {
        path.forEach((object, labels) -> {
            if (object instanceof DetachedElement || object instanceof DetachedProperty || object instanceof DetachedPath) {
                this.objects.add(object);
            } else if (object instanceof Element) {
                this.objects.add(DetachedFactory.detach((Element) object, withProperties));
            } else if (object instanceof Property) {
                this.objects.add(DetachedFactory.detach((Property) object));
            } else if (object instanceof Path) {
                this.objects.add(DetachedFactory.detach((Path) object, withProperties));
            } else {
                this.objects.add(object);
            }
            //Make a copy of the labels as its an UnmodifiableSet which can not be serialized.
            this.labels.add(new LinkedHashSet<>(labels));
        });
    }

    @Override
    public Path attach(final Function<Attachable<Path>, Path> method) {
        final Path path = MutablePath.make();
        this.forEach((object, labels) -> path.extend(object instanceof Attachable ? ((Attachable) object).attach(method) : object, labels));
        return path;
    }
}
