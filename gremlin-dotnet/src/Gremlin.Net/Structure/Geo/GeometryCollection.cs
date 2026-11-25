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
    ///     A geometry collection, which holds any mix of geometry kinds. Geometry collections are immutable.
    /// </summary>
    /// <remarks>
    ///     Deriving from this class outside this library is not supported. The writers and
    ///     <see cref="Equals(Geometry)"/> only recognize the kinds defined here, so another subclass does not
    ///     serialize or compare as a geometry of its own kind.
    /// </remarks>
    public class GeometryCollection : Geometry
    {
        private const int WkbType = 7;

        /// <summary>
        ///     Initializes a new instance of the <see cref="GeometryCollection" /> class from the given geometries.
        ///     All of them must carry the same SRID and z dimension. An element may be empty or itself a collection.
        /// </summary>
        /// <param name="geometries">The geometries, at least 1 of them.</param>
        /// <exception cref="ArgumentException">
        ///     Thrown when the geometries break any of those rules, or when the nesting limit of 32 collection
        ///     levels is exceeded.
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     Thrown when <paramref name="geometries"/> or any of its elements is null.
        /// </exception>
        public GeometryCollection(params Geometry[] geometries) : this(Validated(geometries))
        {
        }

        // The first element of a validated list carries the SRID and z dimension of the whole collection.
        private GeometryCollection(IReadOnlyList<Geometry> geometries)
            : this(geometries, geometries[0].Srid, geometries[0].HasZ)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GeometryCollection" /> class from parts that are already
        ///     validated.
        /// </summary>
        internal GeometryCollection(IReadOnlyList<Geometry> geometries, int srid, bool hasZ) : base(srid, hasZ)
        {
            Geometries = geometries;
        }

        /// <summary>
        ///     Creates an empty geometry collection.
        /// </summary>
        /// <param name="model">The coordinate model.</param>
        /// <returns>An empty geometry collection.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="model"/> is <c>Other</c>.</exception>
        public static GeometryCollection Empty(Model model = Model.Cartesian) =>
            new(Array.Empty<Geometry>(), CoordinateValidator.SridForModel(model), false);

        /// <summary>
        ///     Creates an empty geometry collection with the given SRID.
        /// </summary>
        /// <param name="srid">The spatial reference ID, which is not validated.</param>
        /// <returns>An empty 2D geometry collection carrying that SRID.</returns>
        public static GeometryCollection Empty(int srid) => EmptyWithSrid(srid, false);

        /// <summary>
        ///     Creates an empty geometry collection with an explicit SRID, as the WKB reader needs it.
        /// </summary>
        internal static GeometryCollection EmptyWithSrid(int srid, bool hasZ) =>
            new(Array.Empty<Geometry>(), srid, hasZ);

        /// <summary>
        ///     Creates a geometry collection with an explicit SRID, as the WKB reader needs it. The given list is
        ///     taken over and must not be retained or changed by the caller.
        /// </summary>
        internal static GeometryCollection OfWithSrid(List<Geometry> geometries, int srid, bool hasZ)
        {
            if (geometries.Count == 0)
            {
                return EmptyWithSrid(srid, hasZ);
            }

            foreach (var geometry in geometries)
            {
                CoordinateValidator.ValidateSameSrid(srid, geometry.Srid, "GeometryCollection");
                CoordinateValidator.ValidateSameHasZ(hasZ, geometry.HasZ, "GeometryCollection");
            }

            return new GeometryCollection(geometries.AsReadOnly(), srid, hasZ);
        }

        /// <summary>
        ///     Gets the parts of this collection.
        /// </summary>
        public IReadOnlyList<Geometry> Geometries { get; }

        /// <inheritdoc />
        public override bool IsEmpty
        {
            get
            {
                foreach (var geometry in Geometries)
                {
                    if (!geometry.IsEmpty)
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        /// <inheritdoc />
        public override bool Equals(Geometry? other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (other == null || other.GetType() != GetType()) return false;
            var collection = (GeometryCollection) other;
            if (Srid != collection.Srid) return false;
            if (HasZ != collection.HasZ) return false;
            return CoordinateValidator.PartsEqual(Geometries, collection.Geometries);
        }

        /// <inheritdoc />
        public override int GetHashCode() =>
            CoordinateValidator.PartsHashCode(WkbTypeCode, Srid, HasZ, Geometries);

        /// <inheritdoc />
        internal override int WkbTypeCode => WkbType;

        /// <inheritdoc />
        internal override bool HasNoParts => Geometries.Count == 0;

        /// <inheritdoc />
        internal override string WktTypeName => "GEOMETRYCOLLECTION";

        /// <inheritdoc />
        internal override void WriteWktBody(StringBuilder builder)
        {
            for (var index = 0; index < Geometries.Count; index++)
            {
                if (index > 0)
                {
                    builder.Append(", ");
                }

                WktWriter.WriteTagged(builder, Geometries[index]);
            }
        }

        private static IReadOnlyList<Geometry> Validated(Geometry[] geometries)
        {
            CoordinateValidator.RequireCollection(geometries, nameof(geometries));
            if (geometries.Length == 0)
            {
                throw new ArgumentException("GeometryCollection must have at least 1 geometry;"
                                            + " use GeometryCollection.Empty() for an empty geometry collection",
                    nameof(geometries));
            }

            CoordinateValidator.RequireElement(geometries[0], "GeometryCollection geometries", 0);
            var srid = geometries[0].Srid;
            var hasZ = geometries[0].HasZ;
            var deepestChild = 0;
            for (var index = 0; index < geometries.Length; index++)
            {
                CoordinateValidator.RequireElement(geometries[index], "GeometryCollection geometries", index);
                CoordinateValidator.ValidateSameSrid(srid, geometries[index].Srid, "GeometryCollection");
                CoordinateValidator.ValidateSameHasZ(hasZ, geometries[index].HasZ, "GeometryCollection");
                deepestChild = Math.Max(deepestChild, CoordinateValidator.ComputeDepth(geometries[index]));
            }

            CoordinateValidator.ValidateNestingDepth(1 + deepestChild);
            return Array.AsReadOnly((Geometry[]) geometries.Clone());
        }
    }
}
