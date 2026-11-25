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
    ///     A polygon geometry, an area enclosed by a shell ring with optional hole rings. Polygons are immutable.
    ///     Every ring is closed, so its first and last point are equal, and holds at least 4 points.
    /// </summary>
    public sealed class Polygon : Surface
    {
        private const int WkbType = 3;

        /// <summary>
        ///     Initializes a new instance of the <see cref="Polygon" /> class from a shell ring and any number of
        ///     hole rings. All rings must carry the same SRID and z dimension.
        /// </summary>
        /// <param name="shell">The exterior ring.</param>
        /// <param name="holes">The interior rings.</param>
        /// <exception cref="ArgumentException">
        ///     Thrown when a ring is not closed or holds fewer than 4 points, when a hole is empty or paired with an
        ///     empty shell, or when the rings do not all carry the same SRID and z dimension.
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     Thrown when <paramref name="shell"/>, <paramref name="holes"/> or any of its elements is null.
        /// </exception>
        public Polygon(LineString shell, params LineString[] holes)
            : this(CheckedShell(shell), ValidatedHoles(shell, holes))
        {
        }

        private Polygon(LineString shell, IReadOnlyList<LineString> holes) : base(shell.Srid, shell.HasZ)
        {
            ExteriorRing = shell;
            InteriorRings = holes;
        }

        /// <summary>
        ///     Creates an empty polygon.
        /// </summary>
        /// <param name="model">The coordinate model.</param>
        /// <returns>An empty polygon.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="model"/> is <c>Other</c>.</exception>
        public static Polygon Empty(Model model = Model.Cartesian) =>
            EmptyWithSrid(CoordinateValidator.SridForModel(model), false);

        /// <summary>
        ///     Creates an empty polygon with the given SRID.
        /// </summary>
        /// <param name="srid">The spatial reference ID, which is not validated.</param>
        /// <returns>An empty 2D polygon carrying that SRID.</returns>
        public static Polygon Empty(int srid) => EmptyWithSrid(srid, false);

        /// <summary>
        ///     Creates an empty polygon with an explicit SRID, as the WKB reader needs it.
        /// </summary>
        internal static Polygon EmptyWithSrid(int srid, bool hasZ) =>
            new(LineString.EmptyWithSrid(srid, hasZ), Array.Empty<LineString>());

        /// <summary>
        ///     Creates a polygon with an explicit SRID, as the WKB reader needs it. The given hole list is taken over
        ///     and must not be retained or changed by the caller.
        /// </summary>
        internal static Polygon OfWithSrid(LineString shell, List<LineString> holes, int srid, bool hasZ)
        {
            if (shell.IsEmpty && holes.Count == 0)
            {
                return EmptyWithSrid(srid, hasZ);
            }

            if (!shell.IsEmpty)
            {
                CoordinateValidator.ValidateRing(shell, "Shell ring");
                CoordinateValidator.ValidateSameSrid(srid, shell.Srid, "Polygon");
                CoordinateValidator.ValidateSameHasZ(hasZ, shell.HasZ, "Polygon");
            }

            ValidateHoles(shell, holes);
            return new Polygon(shell, holes.AsReadOnly());
        }

        /// <summary>
        ///     Gets the exterior ring of this polygon, an empty line string when the polygon is empty.
        /// </summary>
        public LineString ExteriorRing { get; }

        /// <summary>
        ///     Gets the interior rings of this polygon.
        /// </summary>
        public IReadOnlyList<LineString> InteriorRings { get; }

        /// <inheritdoc />
        public override bool IsEmpty => ExteriorRing.IsEmpty;

        /// <inheritdoc />
        public override bool Equals(Geometry? other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (other is not Polygon polygon) return false;
            if (Srid != polygon.Srid) return false;
            if (HasZ != polygon.HasZ) return false;
            if (!ExteriorRing.Equals(polygon.ExteriorRing)) return false;
            return CoordinateValidator.PartsEqual(InteriorRings, polygon.InteriorRings);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(WkbType);
            hash.Add(Srid);
            hash.Add(HasZ);
            hash.Add(ExteriorRing);
            foreach (var hole in InteriorRings)
            {
                hash.Add(hole);
            }

            return hash.ToHashCode();
        }

        /// <summary>
        ///     Gets the number of rings this polygon writes, which is 0 when it is empty.
        /// </summary>
        internal int RingCount => ExteriorRing.HasNoParts ? 0 : 1 + InteriorRings.Count;

        /// <inheritdoc />
        internal override int WkbTypeCode => WkbType;

        /// <inheritdoc />
        internal override bool HasNoParts => ExteriorRing.HasNoParts;

        /// <inheritdoc />
        internal override string WktTypeName => "POLYGON";

        /// <inheritdoc />
        internal override void WriteWktBody(StringBuilder builder)
        {
            if (ExteriorRing.HasNoParts)
            {
                return;
            }

            builder.Append('(');
            ExteriorRing.WriteWktBody(builder);
            builder.Append(')');
            foreach (var hole in InteriorRings)
            {
                builder.Append(", (");
                hole.WriteWktBody(builder);
                builder.Append(')');
            }
        }

        private static LineString CheckedShell(LineString shell)
        {
            if (shell == null) throw new ArgumentNullException(nameof(shell));
            CoordinateValidator.ValidateRing(shell, "Shell ring");
            return shell;
        }

        private static IReadOnlyList<LineString> ValidatedHoles(LineString shell, LineString[] holes)
        {
            CoordinateValidator.RequireCollection(holes, nameof(holes));
            ValidateHoles(shell, holes);
            return Array.AsReadOnly((LineString[]) holes.Clone());
        }

        private static void ValidateHoles(LineString shell, IReadOnlyList<LineString> holes)
        {
            for (var index = 0; index < holes.Count; index++)
            {
                var hole = holes[index];
                CoordinateValidator.RequireElement(hole, "Polygon holes", index);
                if (hole.IsEmpty)
                {
                    throw new ArgumentException($"Hole ring at index {index} cannot be empty", nameof(holes));
                }

                if (shell.IsEmpty)
                {
                    throw new ArgumentException("Polygon with an empty shell cannot have holes", nameof(holes));
                }

                CoordinateValidator.ValidateSameSrid(shell.Srid, hole.Srid, "Polygon");
                CoordinateValidator.ValidateSameHasZ(shell.HasZ, hole.HasZ, "Polygon");
                CoordinateValidator.ValidateRing(hole, "Hole ring");
            }
        }
    }
}
