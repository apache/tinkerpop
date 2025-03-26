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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.StringLocalStep;
import org.apache.tinkerpop.gremlin.util.StringUtil;

/**
 * Reference implementation for substring step, a mid-traversal step which returns a list of strings created by
 * splitting the incoming string traverser around the matches of the given separator. A null separator will split the
 * string by whitespaces. Null values from incoming traversers are not processed and remain as null when returned.
 * If the incoming traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
 *
 * @author David Bechberger (http://bechberger.com)
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class SplitLocalStep<S, E> extends StringLocalStep<S, E> implements TraversalParent {

    private final String separator;

    public SplitLocalStep(final Traversal.Admin traversal, final String separator) {
        super(traversal);
        this.separator = separator;
    }

    @Override
    protected E applyStringOperation(String item) {
        return (E) StringUtil.split(item, this.separator);
    }

    @Override
    public String getStepName() { return "split(local)"; }

    public String getSeparator() {
        return this.separator;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (null != this.separator ? this.separator.hashCode() : 0);
        return result;
    }

}
