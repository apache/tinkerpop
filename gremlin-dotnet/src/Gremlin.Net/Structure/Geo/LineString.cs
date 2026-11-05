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
    ///     A line string geometry, a sequence of connected line segments. Line strings are immutable.
    /// </summary>
    public sealed class LineString : Curve
    {
        private const int WkbType = 2;

        /// <summary>
        ///     Initializes a new instance of the <see cref="LineString" /> class from the given points. All points
        ///     must carry the same SRID and z dimension, and none may be empty.
        /// </summary>
        /// <param name="points">The points, at least 2 of them.</param>
        /// <exception cref="ArgumentException">Thrown when the points break any of those rules.</exception>
        /// <exception cref="ArgumentNullException">
        ///     Thrown when <paramref name="points"/> or any of its elements is null.
        /// </exception>
        public LineString(params Point[] points) : this(Validated(points))
        {
        }

        // The first point of a validated list carries the SRID and z dimension of the whole line string.
        private LineString(IReadOnlyList<Point> points) : this(points, points[0].Srid, points[0].HasZ)
        {
        }

        private LineString(IReadOnlyList<Point> points, int srid, bool hasZ) : base(srid, hasZ)
        {
            Points = points;
        }

        /// <summary>
        ///     Creates an empty line string.
        /// </summary>
        /// <param name="model">The coordinate model.</param>
        /// <returns>An empty line string.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="model"/> is <c>Other</c>.</exception>
        public static LineString Empty(Model model = Model.Cartesian) =>
            new(Array.Empty<Point>(), CoordinateValidator.SridForModel(model), false);

        /// <summary>
        ///     Creates an empty line string with the given SRID.
        /// </summary>
        /// <param name="srid">The spatial reference ID, which is not validated.</param>
        /// <returns>An empty 2D line string carrying that SRID.</returns>
        public static LineString Empty(int srid) => EmptyWithSrid(srid, false);

        /// <summary>
        ///     Creates an empty line string with an explicit SRID, as the WKB reader needs it.
        /// </summary>
        internal static LineString EmptyWithSrid(int srid, bool hasZ) => new(Array.Empty<Point>(), srid, hasZ);

        /// <summary>
        ///     Creates a line string with an explicit SRID, as the WKB reader needs it. The given list is taken over
        ///     and must not be retained or changed by the caller.
        /// </summary>
        internal static LineString OfWithSrid(List<Point> points, int srid, bool hasZ)
        {
            if (points.Count == 0)
            {
                return EmptyWithSrid(srid, hasZ);
            }

            if (points.Count == 1)
            {
                throw new ArgumentException("LineString must have at least 2 points, got 1", nameof(points));
            }

            for (var index = 0; index < points.Count; index++)
            {
                CoordinateValidator.ValidateVertex(points[index], "LineString", index);
                CoordinateValidator.ValidateSameSrid(srid, points[index].Srid, "LineString");
                CoordinateValidator.ValidateSameHasZ(hasZ, points[index].HasZ, "LineString");
            }

            return new LineString(points.AsReadOnly(), srid, hasZ);
        }

        /// <summary>
        ///     Gets the points of this line string.
        /// </summary>
        public IReadOnlyList<Point> Points { get; }

        /// <inheritdoc />
        public override bool IsEmpty => Points.Count == 0;

        /// <inheritdoc />
        public override bool Equals(Geometry? other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (other is not LineString lineString) return false;
            if (Srid != lineString.Srid) return false;
            if (HasZ != lineString.HasZ) return false;
            return CoordinateValidator.PartsEqual(Points, lineString.Points);
        }

        /// <inheritdoc />
        public override int GetHashCode() => CoordinateValidator.PartsHashCode(WkbType, Srid, HasZ, Points);

        /// <inheritdoc />
        internal override int WkbTypeCode => WkbType;

        /// <inheritdoc />
        internal override bool HasNoParts => Points.Count == 0;

        /// <inheritdoc />
        internal override string WktTypeName => "LINESTRING";

        /// <inheritdoc />
        internal override void WriteWktBody(StringBuilder builder)
        {
            for (var index = 0; index < Points.Count; index++)
            {
                if (index > 0)
                {
                    builder.Append(", ");
                }

                Points[index].WriteWktBody(builder);
            }
        }

        private static IReadOnlyList<Point> Validated(Point[] points)
        {
            CoordinateValidator.RequireCollection(points, nameof(points));
            if (points.Length == 0)
            {
                throw new ArgumentException("LineString must have at least 2 points, got 0;"
                                            + " use LineString.Empty() for an empty line string", nameof(points));
            }

            if (points.Length == 1)
            {
                throw new ArgumentException("LineString must have at least 2 points, got 1", nameof(points));
            }

            CoordinateValidator.ValidateVertex(points[0], "LineString", 0);
            var srid = points[0].Srid;
            var hasZ = points[0].HasZ;
            for (var index = 1; index < points.Length; index++)
            {
                CoordinateValidator.ValidateVertex(points[index], "LineString", index);
                CoordinateValidator.ValidateSameSrid(srid, points[index].Srid, "LineString");
                CoordinateValidator.ValidateSameHasZ(hasZ, points[index].HasZ, "LineString");
            }

            return Array.AsReadOnly((Point[]) points.Clone());
        }
    }
}
