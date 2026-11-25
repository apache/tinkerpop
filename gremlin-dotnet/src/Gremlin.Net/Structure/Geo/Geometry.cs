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
using System.IO;
using System.Text;

namespace Gremlin.Net.Structure.Geo
{
    /// <summary>
    ///     A base class for all geometry types. Geometry values are immutable.
    /// </summary>
    /// <remarks>
    ///     A geometry is built with the constructors and <c>Empty</c> factory methods of the concrete types, or is
    ///     parsed from ISO WKB with <see cref="FromWkb"/>.
    /// </remarks>
    public abstract class Geometry : IEquatable<Geometry>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="Geometry" /> class.
        /// </summary>
        internal Geometry(int srid, bool hasZ)
        {
            Srid = srid;
            HasZ = hasZ;
        }

        /// <summary>
        ///     Gets the spatial reference ID of this geometry.
        /// </summary>
        public int Srid { get; }

        /// <summary>
        ///     Gets a value indicating whether this geometry carries z coordinates.
        /// </summary>
        public bool HasZ { get; }

        /// <summary>
        ///     Gets the coordinate interpretation model that follows from the SRID.
        /// </summary>
        public Model Model => CoordinateValidator.ModelForSrid(Srid);

        /// <summary>
        ///     Gets a value indicating whether this geometry holds no coordinates. A collection is empty when all of
        ///     its parts are empty.
        /// </summary>
        public abstract bool IsEmpty { get; }

        /// <summary>
        ///     Parses one ISO WKB geometry with the given SRID.
        /// </summary>
        /// <remarks>
        ///     EWKB is rejected. A point whose x and y are both <c>NaN</c> reads as an empty point, which is allowed
        ///     on its own and in a <see cref="MultiPoint"/>, but not as a line string or ring vertex.
        /// </remarks>
        /// <param name="wkb">The ISO WKB bytes of exactly one geometry.</param>
        /// <param name="srid">The SRID to apply. It is not validated.</param>
        /// <returns>The parsed geometry.</returns>
        /// <exception cref="ArgumentException">
        ///     Thrown when the bytes are empty, truncated, followed by extra bytes or not valid ISO WKB, or when the
        ///     geometry breaks a rule the constructors enforce.
        /// </exception>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="wkb"/> is null.</exception>
        public static Geometry FromWkb(byte[] wkb, int srid)
        {
            if (wkb == null) throw new ArgumentNullException(nameof(wkb));
            if (wkb.Length == 0) throw new ArgumentException("WKB input cannot be empty", nameof(wkb));

            using var stream = new MemoryStream(wkb, false);
            Geometry geometry;
            try
            {
                // Reads from a MemoryStream complete synchronously, so waiting here does not block on I/O.
                geometry = WkbReader.ReadAsync(stream, srid).AsTask().GetAwaiter().GetResult();
            }
            catch (IOException e)
            {
                throw new ArgumentException("Truncated WKB input: unexpected end of input", nameof(wkb), e);
            }

            if (stream.Position != wkb.Length)
            {
                throw new ArgumentException("Unexpected trailing bytes in WKB input", nameof(wkb));
            }

            return geometry;
        }

        /// <summary>
        ///     Returns the ISO WKT text of this geometry, without an SRID prefix.
        /// </summary>
        /// <returns>The WKT text.</returns>
        public string ToWkt() => WktWriter.Write(this);

        /// <summary>
        ///     Returns the EWKT text of this geometry, which is its WKT text behind an <c>SRID=n;</c> prefix.
        /// </summary>
        /// <returns>The EWKT text.</returns>
        public string ToEwkt() => $"SRID={Srid};{ToWkt()}";

        /// <summary>
        ///     Returns the ISO WKB bytes of this geometry, in big-endian byte order.
        /// </summary>
        /// <returns>The WKB bytes.</returns>
        public byte[] ToWkb() => WkbWriter.Write(this);

        /// <summary>
        ///     Determines whether this geometry equals another one. Two geometries are equal when they are of the
        ///     same kind and hold the same SRID, z dimension and coordinates. Coordinates are compared numerically,
        ///     so -0.0 equals 0.0, and two empty points are equal despite their NaN coordinates.
        /// </summary>
        /// <param name="other">The geometry to compare with.</param>
        /// <returns>True if the geometries are equal; otherwise, false.</returns>
        public abstract bool Equals(Geometry? other);

        /// <inheritdoc />
        public override bool Equals(object? obj) => obj is Geometry other && Equals(other);

        /// <inheritdoc />
        public abstract override int GetHashCode();

        /// <summary>
        ///     Determines whether two specified geometries are equal.
        /// </summary>
        /// <param name="first">The first geometry.</param>
        /// <param name="second">The second geometry.</param>
        /// <returns>True if the geometries are equal; otherwise, false.</returns>
        public static bool operator ==(Geometry? first, Geometry? second)
        {
            if (ReferenceEquals(null, first))
            {
                return ReferenceEquals(null, second);
            }

            return first.Equals(second);
        }

        /// <summary>
        ///     Determines whether two specified geometries are different.
        /// </summary>
        /// <param name="first">The first geometry.</param>
        /// <param name="second">The second geometry.</param>
        /// <returns>True if the geometries are different; otherwise, false.</returns>
        public static bool operator !=(Geometry? first, Geometry? second) => !(first == second);

        /// <summary>
        ///     Returns the WKT text of this geometry.
        /// </summary>
        /// <returns>The WKT text.</returns>
        public override string ToString() => ToWkt();

        /// <summary>
        ///     Gets the WKB type code of this geometry kind.
        /// </summary>
        internal abstract int WkbTypeCode { get; }

        /// <summary>
        ///     Gets a value indicating whether this geometry holds no parts of its own. The writers use this
        ///     shallow test to choose between <c>X EMPTY</c> and a parenthesized body. It differs from
        ///     <see cref="IsEmpty"/>, which is deep: a collection whose parts are all empty is empty but still has
        ///     parts.
        /// </summary>
        internal abstract bool HasNoParts { get; }

        /// <summary>
        ///     Gets the WKT type name of this geometry kind.
        /// </summary>
        internal abstract string WktTypeName { get; }

        /// <summary>
        ///     Appends the WKT body of this geometry to the given builder.
        /// </summary>
        internal abstract void WriteWktBody(StringBuilder builder);
    }
}
