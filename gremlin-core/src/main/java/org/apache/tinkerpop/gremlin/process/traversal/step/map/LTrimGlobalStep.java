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
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.Set;

/**
 * Reference implementation for lTrim() step, a mid-traversal step which returns a string with leading
 * whitespace removed. Null values are not processed and remain as null when returned.
 * If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
 *
 * @author David Bechberger (http://bechberger.com)
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class LTrimGlobalStep<S, E> extends ScalarMapStep<S, E> {

    public LTrimGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final S item = traverser.get();
        // throws when incoming traverser isn't a string
        if (null != item && !(item instanceof String)) {
            throw new IllegalArgumentException(
                    String.format("The lTrim() step can only take string as argument, encountered %s", item.getClass()));
        }

        // we will pass null values to next step
        if (null == item)
                return null;

        int i = 0;
        while (i < ((String) item).length() && Character.isWhitespace(((String) item).charAt(i))) {
            i++;
        }

        return (E) ((String) item).substring(i);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

}
