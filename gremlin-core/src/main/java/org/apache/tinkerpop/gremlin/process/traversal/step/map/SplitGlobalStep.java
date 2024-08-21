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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.StringUtil;

import java.util.Collections;
import java.util.Set;

/**
 * Reference implementation for substring step, a mid-traversal step which returns a list of strings created by
 * splitting the incoming string traverser around the matches of the given separator. A null separator will split the
 * string by whitespaces. Null values from incoming traversers are not processed and remain as null when returned.
 * If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
 *
 * @author David Bechberger (http://bechberger.com)
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class SplitGlobalStep<S, E> extends ScalarMapStep<S, E> implements TraversalParent {

    private final String separator;

    public SplitGlobalStep(final Traversal.Admin traversal, final String separator) {
        super(traversal);
        this.separator = separator;
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final S item = traverser.get();
        // throws when incoming traverser isn't a string
        if (null != item && !(item instanceof String)) {
            throw new IllegalArgumentException(
                    String.format("The split() step can only take string as argument, encountered %s.", item.getClass()));
        }

        // we will pass null values to next step
        return null == item? null : (E) StringUtil.split((String)item, this.separator);
    }

    public String getSeparator() {
        return this.separator;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (null != this.separator ? this.separator.hashCode() : 0);
        return result;
    }

}
