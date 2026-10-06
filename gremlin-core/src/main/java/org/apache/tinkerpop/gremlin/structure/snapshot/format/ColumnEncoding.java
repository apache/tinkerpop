/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.snapshot.format;

/**
 * How a column directory stores its entries. A column is dense when {@code presentCount * 4 >= elementCount} and sparse
 * otherwise. Mixed-type columns use a variable-width encoding plus a {@code types.bin} segment.
 */
public enum ColumnEncoding {

    /**
     * Dense, single fixed-width type: optional {@code presence.bin} and {@code values.bin}.
     */
    DENSE_FIXED,

    /**
     * Sparse, single fixed-width type: {@code ordinals.bin} and {@code values.bin}.
     */
    SPARSE_FIXED,

    /**
     * Dense, variable-width or mixed types: optional {@code presence.bin}, {@code offsets.bin} and {@code data.bin}.
     */
    DENSE_VARIABLE,

    /**
     * Sparse, variable-width or mixed types: {@code ordinals.bin}, {@code offsets.bin} and {@code data.bin}.
     */
    SPARSE_VARIABLE
}
