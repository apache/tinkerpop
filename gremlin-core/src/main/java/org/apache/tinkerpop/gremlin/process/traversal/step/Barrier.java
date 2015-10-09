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

package org.apache.tinkerpop.gremlin.process.traversal.step;

/**
 * A Barrier is any step that requires all left traversers to be processed prior to emitting result traversers to the right.
 * Note that some barrier steps may be "lazy" in that if their algorithm permits, they can emit right traversers prior to all traversers being aggregated.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Barrier {

    /**
     * Process all left traversers by do not yield the resultant output.
     * This method is useful for steps like {@link org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep}, where traversers can be processed "on the fly" and thus, reduce memory consumption.
     */
    public void processAllStarts();
}
