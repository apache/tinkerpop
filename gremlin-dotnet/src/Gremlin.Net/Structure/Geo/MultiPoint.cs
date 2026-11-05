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
    ///     A multi-point geometry, which holds any number of points. Multi-points are immutable.
    /// </summary>
    public sealed class MultiPoint : GeometryCollection
    {
        private const int WkbType = 4;

        /// <summary>
        ///     Initializes a new instance of the <see cref="MultiPoint" /> class from the given points. All points
        ///     must carry the same SRID and z dimension. An element may be an empty point.
        /// </summary>
        /// <param name="points">The points, at least 1 of them.</param>
        /// <exception cref="ArgumentException">Thrown when the points break any of those rules.</exception>
        /// <exception cref="ArgumentNullException">
        ///     Thrown when <paramref name="points"/> or any of its elements is null.
        /// </exception>
        public MultiPoint(params Point[] points) : this(Validated(points))
        {
        }

        // The first point of a validated list carries the SRID and z dimension of the whole multi-point.
        private MultiPoint(IReadOnlyList<Point> points) : this(points, points[0].Srid, points[0].HasZ)
        {
        }

        private MultiPoint(IReadOnlyList<Point> points, int srid, bool hasZ) : base(points, srid, hasZ)
        {
            Points = points;
        }

        /// <summary>
        ///     Creates an empty multi-point.
        /// </summary>
        /// <param name="model">The coordinate model.</param>
        /// <returns>An empty multi-point.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="model"/> is <c>Other</c>.</exception>
        public static new MultiPoint Empty(Model model = Model.Cartesian) =>
            new(Array.Empty<Point>(), CoordinateValidator.SridForModel(model), false);

        /// <summary>
        ///     Creates an empty multi-point with the given SRID.
        /// </summary>
        /// <param name="srid">The spatial reference ID, which is not validated.</param>
        /// <returns>An empty 2D multi-point carrying that SRID.</returns>
        public static new MultiPoint Empty(int srid) => EmptyWithSrid(srid, false);

        /// <summary>
        ///     Creates an empty multi-point with an explicit SRID, as the WKB reader needs it.
        /// </summary>
        internal static new MultiPoint EmptyWithSrid(int srid, bool hasZ) => new(Array.Empty<Point>(), srid, hasZ);

        /// <summary>
        ///     Creates a multi-point with an explicit SRID, as the WKB reader needs it. The given list is taken over
        ///     and must not be retained or changed by the caller.
        /// </summary>
        internal static MultiPoint OfWithSrid(List<Point> points, int srid, bool hasZ)
        {
            if (points.Count == 0)
            {
                return EmptyWithSrid(srid, hasZ);
            }

            foreach (var point in points)
            {
                CoordinateValidator.ValidateSameSrid(srid, point.Srid, "MultiPoint");
                CoordinateValidator.ValidateSameHasZ(hasZ, point.HasZ, "MultiPoint");
            }

            return new MultiPoint(points.AsReadOnly(), srid, hasZ);
        }

        /// <summary>
        ///     Gets the points of this multi-point.
        /// </summary>
        public IReadOnlyList<Point> Points { get; }

        /// <inheritdoc />
        public override bool Equals(Geometry? other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (other is not MultiPoint multiPoint) return false;
            if (Srid != multiPoint.Srid) return false;
            if (HasZ != multiPoint.HasZ) return false;
            return CoordinateValidator.PartsEqual(Points, multiPoint.Points);
        }

        /// <inheritdoc />
        public override int GetHashCode() => CoordinateValidator.PartsHashCode(WkbType, Srid, HasZ, Points);

        /// <inheritdoc />
        internal override int WkbTypeCode => WkbType;

        /// <inheritdoc />
        internal override string WktTypeName => "MULTIPOINT";

        /// <inheritdoc />
        internal override void WriteWktBody(StringBuilder builder)
        {
            for (var index = 0; index < Points.Count; index++)
            {
                if (index > 0)
                {
                    builder.Append(", ");
                }

                var point = Points[index];
                if (point.HasNoParts)
                {
                    builder.Append("EMPTY");
                }
                else
                {
                    builder.Append('(');
                    point.WriteWktBody(builder);
                    builder.Append(')');
                }
            }
        }

        private static IReadOnlyList<Point> Validated(Point[] points)
        {
            CoordinateValidator.RequireCollection(points, nameof(points));
            if (points.Length == 0)
            {
                throw new ArgumentException("MultiPoint must have at least 1 point;"
                                            + " use MultiPoint.Empty() for an empty multi-point", nameof(points));
            }

            CoordinateValidator.RequireElement(points[0], "MultiPoint points", 0);
            var srid = points[0].Srid;
            var hasZ = points[0].HasZ;
            for (var index = 1; index < points.Length; index++)
            {
                CoordinateValidator.RequireElement(points[index], "MultiPoint points", index);
                CoordinateValidator.ValidateSameSrid(srid, points[index].Srid, "MultiPoint");
                CoordinateValidator.ValidateSameHasZ(hasZ, points[index].HasZ, "MultiPoint");
            }

            return Array.AsReadOnly((Point[]) points.Clone());
        }
    }
}
