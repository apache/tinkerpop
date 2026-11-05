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
    ///     A multi-line string geometry, which holds any number of line strings. Multi-line strings are immutable.
    /// </summary>
    public sealed class MultiLineString : MultiCurve
    {
        private const int WkbType = 5;

        /// <summary>
        ///     Initializes a new instance of the <see cref="MultiLineString" /> class from the given line strings.
        ///     All of them must carry the same SRID and z dimension. An element may be an empty line string.
        /// </summary>
        /// <param name="lineStrings">The line strings, at least 1 of them.</param>
        /// <exception cref="ArgumentException">Thrown when the line strings break any of those rules.</exception>
        /// <exception cref="ArgumentNullException">
        ///     Thrown when <paramref name="lineStrings"/> or any of its elements is null.
        /// </exception>
        public MultiLineString(params LineString[] lineStrings) : this(Validated(lineStrings))
        {
        }

        // The first element of a validated list carries the SRID and z dimension of the whole multi-line string.
        private MultiLineString(IReadOnlyList<LineString> lineStrings)
            : this(lineStrings, lineStrings[0].Srid, lineStrings[0].HasZ)
        {
        }

        private MultiLineString(IReadOnlyList<LineString> lineStrings, int srid, bool hasZ)
            : base(lineStrings, srid, hasZ)
        {
            LineStrings = lineStrings;
        }

        /// <summary>
        ///     Creates an empty multi-line string.
        /// </summary>
        /// <param name="model">The coordinate model.</param>
        /// <returns>An empty multi-line string.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="model"/> is <c>Other</c>.</exception>
        public static new MultiLineString Empty(Model model = Model.Cartesian) =>
            new(Array.Empty<LineString>(), CoordinateValidator.SridForModel(model), false);

        /// <summary>
        ///     Creates an empty multi-line string with the given SRID.
        /// </summary>
        /// <param name="srid">The spatial reference ID, which is not validated.</param>
        /// <returns>An empty 2D multi-line string carrying that SRID.</returns>
        public static new MultiLineString Empty(int srid) => EmptyWithSrid(srid, false);

        /// <summary>
        ///     Creates an empty multi-line string with an explicit SRID, as the WKB reader needs it.
        /// </summary>
        internal static new MultiLineString EmptyWithSrid(int srid, bool hasZ) =>
            new(Array.Empty<LineString>(), srid, hasZ);

        /// <summary>
        ///     Creates a multi-line string with an explicit SRID, as the WKB reader needs it. The given list is taken
        ///     over and must not be retained or changed by the caller.
        /// </summary>
        internal static MultiLineString OfWithSrid(List<LineString> lineStrings, int srid, bool hasZ)
        {
            if (lineStrings.Count == 0)
            {
                return EmptyWithSrid(srid, hasZ);
            }

            foreach (var lineString in lineStrings)
            {
                CoordinateValidator.ValidateSameSrid(srid, lineString.Srid, "MultiLineString");
                CoordinateValidator.ValidateSameHasZ(hasZ, lineString.HasZ, "MultiLineString");
            }

            return new MultiLineString(lineStrings.AsReadOnly(), srid, hasZ);
        }

        /// <summary>
        ///     Gets the line strings of this multi-line string.
        /// </summary>
        public IReadOnlyList<LineString> LineStrings { get; }

        /// <inheritdoc />
        public override bool Equals(Geometry? other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (other is not MultiLineString multiLineString) return false;
            if (Srid != multiLineString.Srid) return false;
            if (HasZ != multiLineString.HasZ) return false;
            return CoordinateValidator.PartsEqual(LineStrings, multiLineString.LineStrings);
        }

        /// <inheritdoc />
        public override int GetHashCode() => CoordinateValidator.PartsHashCode(WkbType, Srid, HasZ, LineStrings);

        /// <inheritdoc />
        internal override int WkbTypeCode => WkbType;

        /// <inheritdoc />
        internal override string WktTypeName => "MULTILINESTRING";

        /// <inheritdoc />
        internal override void WriteWktBody(StringBuilder builder)
        {
            for (var index = 0; index < LineStrings.Count; index++)
            {
                if (index > 0)
                {
                    builder.Append(", ");
                }

                var lineString = LineStrings[index];
                if (lineString.HasNoParts)
                {
                    builder.Append("EMPTY");
                }
                else
                {
                    builder.Append('(');
                    lineString.WriteWktBody(builder);
                    builder.Append(')');
                }
            }
        }

        private static IReadOnlyList<LineString> Validated(LineString[] lineStrings)
        {
            CoordinateValidator.RequireCollection(lineStrings, nameof(lineStrings));
            if (lineStrings.Length == 0)
            {
                throw new ArgumentException("MultiLineString must have at least 1 line string;"
                                            + " use MultiLineString.Empty() for an empty multi-line string",
                    nameof(lineStrings));
            }

            CoordinateValidator.RequireElement(lineStrings[0], "MultiLineString line strings", 0);
            var srid = lineStrings[0].Srid;
            var hasZ = lineStrings[0].HasZ;
            for (var index = 1; index < lineStrings.Length; index++)
            {
                CoordinateValidator.RequireElement(lineStrings[index], "MultiLineString line strings", index);
                CoordinateValidator.ValidateSameSrid(srid, lineStrings[index].Srid, "MultiLineString");
                CoordinateValidator.ValidateSameHasZ(hasZ, lineStrings[index].HasZ, "MultiLineString");
            }

            return Array.AsReadOnly((LineString[]) lineStrings.Clone());
        }
    }
}
