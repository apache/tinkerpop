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

import java.util.Collections;
import java.util.Set;

/**
 * Reference implementation for substring step, a mid-traversal step which returns a substring of the incoming string
 * traverser with a 0-based start index (inclusive) and length specified. If the start index is negative then it will
 * begin at the specified index counted from the end of the string, or 0 if exceeding the string length.
 * Length is optional, if it is not specific or if it exceeds the length of the string then all remaining characters will
 * be returned. Length <= 0 will return the empty string. Null values are not processed and remain as null when returned.
 * If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
 *
 * @author David Bechberger (http://bechberger.com)
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class SubstringStep<S> extends ScalarMapStep<S, String> {

    private final Integer start;
    private final Integer length;

    public SubstringStep(final Traversal.Admin traversal, final Integer startIndex, final Integer length) {
        super(traversal);
        this.start = startIndex;
        this.length = length;
    }

    public SubstringStep(final Traversal.Admin traversal, final Integer startIndex) {
        this(traversal, startIndex, null);
    }

    @Override
    protected String map(final Traverser.Admin<S> traverser) {
        final S item = traverser.get();
        // throws when incoming traverser isn't a string
        if (null != item && !(item instanceof String)) {
            throw new IllegalArgumentException(
                    String.format("The substring() step can only take string as argument, encountered %s", item.getClass()));
        }

        // to preserve null items
        final String strItem = (String) item;

        if (null == strItem)
            return null;

        final int newStart = processStartIndex(strItem.length());
        if (null == this.length)
            return strItem.substring(newStart);

        // length < 0 will return the empty string.
        if (this.length <= 0)
            return "";

        // if length specified exceeds the string length it is assumed to be equal to the length, which returns all
        // remaining characters in the string.
        return strItem.substring(newStart, Math.min(this.length + newStart, strItem.length()));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + this.start.hashCode();
        result = 31 * result + (null != this.length ? this.length.hashCode() : 0);
        return result;
    }

    // Helper function to process the start index, if it is negative (which counts from end of string) it is converted
    // to the positive index position or 0 when negative index exceeds the string length, if it is positive and exceeds
    // the length of the string, it is assumed to equal to the length, which means an empty string will be returned.
    private int processStartIndex(int strLen) {
        if (this.start < 0) {
            return Math.max(0, (strLen + this.start) % strLen);
        } else {
            return Math.min(this.start, strLen);
        }
    }

}
