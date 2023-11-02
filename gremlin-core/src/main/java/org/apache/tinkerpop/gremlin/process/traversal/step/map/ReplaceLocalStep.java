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

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.StringLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.Set;

/**
 * Reference implementation for substring step, a mid-traversal step which returns a string with the specified
 * characters in the original string replaced with the new characters. Any null arguments will be a no-op and the
 * original string is returned. Null values are not processed and remain as null when returned. If the incoming
 * traverser is a non-String value then an {@code IllegalArgumentException} will be thrown.
 *
 * @author David Bechberger (http://bechberger.com)
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class ReplaceLocalStep<S, E> extends StringLocalStep<S, E> {

    private final String oldChar;
    private final String newChar;

    public ReplaceLocalStep(final Traversal.Admin traversal, final String oldChar, final String newChar ) {
        super(traversal);
        this.oldChar=oldChar;
        this.newChar=newChar;
    }

    @Override
    protected E applyStringOperation(String item) {
        return (E) StringUtils.replace(item, this.oldChar, this.newChar);
    }

    @Override
    public String getStepName() { return "replace(local)"; }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (null != this.oldChar ? this.oldChar.hashCode() : 0);
        result = 31 * result + (null != this.newChar ? this.newChar.hashCode() : 0);
        return result;
    }

}
