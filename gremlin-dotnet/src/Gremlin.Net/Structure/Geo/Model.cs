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

namespace Gremlin.Net.Structure.Geo
{
    /// <summary>
    ///     Defines the coordinate interpretation model of a geometry.
    /// </summary>
    public enum Model
    {
        /// <summary>
        ///     Planar coordinates with math in coordinate units. SRID 0.
        /// </summary>
        Cartesian,

        /// <summary>
        ///     Longitude and latitude on the WGS84 earth surface. SRID 4326.
        /// </summary>
        Geographic,

        /// <summary>
        ///     Any other spatial reference system, reported for SRIDs other than 0 and 4326. Constructors reject
        ///     it, so build such a geometry with <see cref="Point.WithSrid(double, double, int)"/> or with
        ///     <see cref="Geometry.FromWkb"/> and its SRID.
        /// </summary>
        Other
    }
}
