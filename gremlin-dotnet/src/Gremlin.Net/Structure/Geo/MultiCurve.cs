#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System.Collections.Generic;

namespace Gremlin.Net.Structure.Geo
{
    /// <summary>
    ///     A base class for the collections of curves. <see cref="MultiLineString"/> is the only one.
    /// </summary>
    public abstract class MultiCurve : GeometryCollection
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="MultiCurve" /> class.
        /// </summary>
        internal MultiCurve(IReadOnlyList<Geometry> geometries, int srid, bool hasZ)
            : base(geometries, srid, hasZ)
        {
        }
    }
}
