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
package com.apache.tinkerpop.gremlin.process.traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StepPosition {

    public int x; // step in traversal length
    public int y; // depth in traversal nested tree
    public int z; // breadth in traversal siblings
    public String parentId; // the traversal holder id

    private StepPosition(final int x, final int y, final int z, final String parentId) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.parentId = parentId;
    }

    public StepPosition() {
        this(0, 0, 0, "");
    }

    public String nextXId() {
        return this.x++ + "." + this.y + '.' + this.z + '(' + this.parentId + ')';
    }

    @Override
    public String toString() {
        return this.x + "." + this.y + '.' + this.z + '(' + this.parentId + ')';
    }
}
