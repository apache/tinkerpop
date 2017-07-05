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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StepPosition implements Serializable {

    public int x; // step in traversal length
    public int y; // depth in traversal nested tree
    public int z; // breadth in traversal siblings
    public String parentId; // the traversal holder id

    private static final String DOT = ".";
    private static final String LEFT_PARENTHESES = "(";
    private static final String RIGHT_PARENTHESES = ")";
    private static final String EMPTY_STRING = "";

    private StepPosition(final int x, final int y, final int z, final String parentId) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.parentId = parentId;
    }

    public StepPosition() {
        this(0, 0, 0, EMPTY_STRING);
    }

    public String nextXId() {
        return this.x++ + DOT + this.y + DOT + this.z + LEFT_PARENTHESES + this.parentId + RIGHT_PARENTHESES;
    }

    @Override
    public String toString() {
        return this.x + DOT + this.y + DOT + this.z + LEFT_PARENTHESES + this.parentId + RIGHT_PARENTHESES;
    }

    public static boolean isStepId(final String maybeAStepId) {
        return maybeAStepId.matches("[0-9]+\\.[0-9]+\\.[0-9]+\\([0-9\\.\\(\\)]*\\)");
    }
}
