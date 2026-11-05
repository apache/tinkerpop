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

using System;
using System.Collections.Generic;
using System.Text;

namespace Gremlin.Net.Structure.Geo
{
    /// <summary>
    ///     A multi-polygon geometry, which holds any number of polygons. Multi-polygons are immutable.
    /// </summary>
    public sealed class MultiPolygon : MultiSurface
    {
        private const int WkbType = 6;

        /// <summary>
        ///     Initializes a new instance of the <see cref="MultiPolygon" /> class from the given polygons. All of
        ///     them must carry the same SRID and z dimension. An element may be an empty polygon.
        /// </summary>
        /// <param name="polygons">The polygons, at least 1 of them.</param>
        /// <exception cref="ArgumentException">Thrown when the polygons break any of those rules.</exception>
        /// <exception cref="ArgumentNullException">
        ///     Thrown when <paramref name="polygons"/> or any of its elements is null.
        /// </exception>
        public MultiPolygon(params Polygon[] polygons) : this(Validated(polygons))
        {
        }

        // The first polygon of a validated list carries the SRID and z dimension of the whole multi-polygon.
        private MultiPolygon(IReadOnlyList<Polygon> polygons)
            : this(polygons, polygons[0].Srid, polygons[0].HasZ)
        {
        }

        private MultiPolygon(IReadOnlyList<Polygon> polygons, int srid, bool hasZ) : base(polygons, srid, hasZ)
        {
            Polygons = polygons;
        }

        /// <summary>
        ///     Creates an empty multi-polygon.
        /// </summary>
        /// <param name="model">The coordinate model.</param>
        /// <returns>An empty multi-polygon.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="model"/> is <c>Other</c>.</exception>
        public static new MultiPolygon Empty(Model model = Model.Cartesian) =>
            new(Array.Empty<Polygon>(), CoordinateValidator.SridForModel(model), false);

        /// <summary>
        ///     Creates an empty multi-polygon with the given SRID.
        /// </summary>
        /// <param name="srid">The spatial reference ID, which is not validated.</param>
        /// <returns>An empty 2D multi-polygon carrying that SRID.</returns>
        public static new MultiPolygon Empty(int srid) => EmptyWithSrid(srid, false);

        /// <summary>
        ///     Creates an empty multi-polygon with an explicit SRID, as the WKB reader needs it.
        /// </summary>
        internal static new MultiPolygon EmptyWithSrid(int srid, bool hasZ) => new(Array.Empty<Polygon>(), srid, hasZ);

        /// <summary>
        ///     Creates a multi-polygon with an explicit SRID, as the WKB reader needs it. The given list is taken
        ///     over and must not be retained or changed by the caller.
        /// </summary>
        internal static MultiPolygon OfWithSrid(List<Polygon> polygons, int srid, bool hasZ)
        {
            if (polygons.Count == 0)
            {
                return EmptyWithSrid(srid, hasZ);
            }

            foreach (var polygon in polygons)
            {
                CoordinateValidator.ValidateSameSrid(srid, polygon.Srid, "MultiPolygon");
                CoordinateValidator.ValidateSameHasZ(hasZ, polygon.HasZ, "MultiPolygon");
            }

            return new MultiPolygon(polygons.AsReadOnly(), srid, hasZ);
        }

        /// <summary>
        ///     Gets the polygons of this multi-polygon.
        /// </summary>
        public IReadOnlyList<Polygon> Polygons { get; }

        /// <inheritdoc />
        public override bool Equals(Geometry? other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (other is not MultiPolygon multiPolygon) return false;
            if (Srid != multiPolygon.Srid) return false;
            if (HasZ != multiPolygon.HasZ) return false;
            return CoordinateValidator.PartsEqual(Polygons, multiPolygon.Polygons);
        }

        /// <inheritdoc />
        public override int GetHashCode() => CoordinateValidator.PartsHashCode(WkbType, Srid, HasZ, Polygons);

        /// <inheritdoc />
        internal override int WkbTypeCode => WkbType;

        /// <inheritdoc />
        internal override string WktTypeName => "MULTIPOLYGON";

        /// <inheritdoc />
        internal override void WriteWktBody(StringBuilder builder)
        {
            for (var index = 0; index < Polygons.Count; index++)
            {
                if (index > 0)
                {
                    builder.Append(", ");
                }

                var polygon = Polygons[index];
                if (polygon.HasNoParts)
                {
                    builder.Append("EMPTY");
                }
                else
                {
                    builder.Append('(');
                    polygon.WriteWktBody(builder);
                    builder.Append(')');
                }
            }
        }

        private static IReadOnlyList<Polygon> Validated(Polygon[] polygons)
        {
            CoordinateValidator.RequireCollection(polygons, nameof(polygons));
            if (polygons.Length == 0)
            {
                throw new ArgumentException("MultiPolygon must have at least 1 polygon;"
                                            + " use MultiPolygon.Empty() for an empty multi-polygon",
                    nameof(polygons));
            }

            CoordinateValidator.RequireElement(polygons[0], "MultiPolygon polygons", 0);
            var srid = polygons[0].Srid;
            var hasZ = polygons[0].HasZ;
            for (var index = 1; index < polygons.Length; index++)
            {
                CoordinateValidator.RequireElement(polygons[index], "MultiPolygon polygons", index);
                CoordinateValidator.ValidateSameSrid(srid, polygons[index].Srid, "MultiPolygon");
                CoordinateValidator.ValidateSameHasZ(hasZ, polygons[index].HasZ, "MultiPolygon");
            }

            return Array.AsReadOnly((Polygon[]) polygons.Clone());
        }
    }
}
