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
using System.Text;

namespace Gremlin.Net.Structure.Geo
{
    /// <summary>
    ///     A point geometry, a single location in 2D or 3D space. Points are immutable.
    /// </summary>
    public sealed class Point : Geometry
    {
        private const int WkbType = 1;

        private readonly double _x;
        private readonly double _y;
        private readonly double _z;
        private readonly bool _empty;

        /// <summary>
        ///     Initializes a new instance of the <see cref="Point" /> class holding a 2D coordinate.
        /// </summary>
        /// <param name="x">The x coordinate, the longitude under the geographic model.</param>
        /// <param name="y">The y coordinate, the latitude under the geographic model.</param>
        /// <param name="model">The coordinate model.</param>
        /// <exception cref="ArgumentException">
        ///     Thrown when a coordinate is not finite, when a geographic coordinate is out of range, or when
        ///     <paramref name="model"/> is <c>Other</c>.
        /// </exception>
        public Point(double x, double y, Model model = Model.Cartesian)
            : base(CoordinateValidator.SridForModel(model), false)
        {
            CoordinateValidator.ValidateX(x, Srid);
            CoordinateValidator.ValidateY(y, Srid);
            _x = x;
            _y = y;
            _z = double.NaN;
            _empty = false;
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="Point" /> class holding a 3D coordinate.
        /// </summary>
        /// <param name="x">The x coordinate, the longitude under the geographic model.</param>
        /// <param name="y">The y coordinate, the latitude under the geographic model.</param>
        /// <param name="z">The z coordinate, the elevation.</param>
        /// <param name="model">The coordinate model.</param>
        /// <exception cref="ArgumentException">
        ///     Thrown when a coordinate is not finite, when a geographic coordinate is out of range, or when
        ///     <paramref name="model"/> is <c>Other</c>.
        /// </exception>
        public Point(double x, double y, double z, Model model = Model.Cartesian)
            : base(CoordinateValidator.SridForModel(model), true)
        {
            CoordinateValidator.ValidateX(x, Srid);
            CoordinateValidator.ValidateY(y, Srid);
            CoordinateValidator.ValidateZ(z);
            _x = x;
            _y = y;
            _z = z;
            _empty = false;
        }

        private Point(int srid, bool hasZ, bool empty, double x, double y, double z) : base(srid, hasZ)
        {
            _x = x;
            _y = y;
            _z = z;
            _empty = empty;
        }

        /// <summary>
        ///     Creates an empty 2D point.
        /// </summary>
        /// <param name="model">The coordinate model.</param>
        /// <returns>An empty point.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="model"/> is <c>Other</c>.</exception>
        public static Point Empty(Model model = Model.Cartesian) =>
            new(CoordinateValidator.SridForModel(model), false, true, double.NaN, double.NaN, double.NaN);

        /// <summary>
        ///     Creates an empty point with the given SRID.
        /// </summary>
        /// <param name="srid">The spatial reference ID, which is not validated.</param>
        /// <returns>An empty 2D point carrying that SRID.</returns>
        public static Point Empty(int srid) => EmptyWithSrid(srid, false);

        /// <summary>
        ///     Creates a 2D point with the given SRID, for a spatial reference system that <see cref="Geo.Model" />
        ///     does not name.
        /// </summary>
        /// <param name="x">The x coordinate, the longitude when <paramref name="srid"/> is 4326.</param>
        /// <param name="y">The y coordinate, the latitude when <paramref name="srid"/> is 4326.</param>
        /// <param name="srid">The spatial reference ID, which is not validated.</param>
        /// <returns>A point carrying that SRID.</returns>
        /// <exception cref="ArgumentException">
        ///     Thrown when a coordinate is not finite, or when <paramref name="srid"/> is 4326 and a coordinate is
        ///     outside the geographic range.
        /// </exception>
        public static Point WithSrid(double x, double y, int srid) => OfWithSrid(x, y, srid);

        /// <summary>
        ///     Creates a 3D point with the given SRID, for a spatial reference system that <see cref="Geo.Model" />
        ///     does not name.
        /// </summary>
        /// <param name="x">The x coordinate, the longitude when <paramref name="srid"/> is 4326.</param>
        /// <param name="y">The y coordinate, the latitude when <paramref name="srid"/> is 4326.</param>
        /// <param name="z">The z coordinate, the elevation.</param>
        /// <param name="srid">The spatial reference ID, which is not validated.</param>
        /// <returns>A point carrying that SRID.</returns>
        /// <exception cref="ArgumentException">
        ///     Thrown when a coordinate is not finite, or when <paramref name="srid"/> is 4326 and a coordinate is
        ///     outside the geographic range.
        /// </exception>
        public static Point WithSrid(double x, double y, double z, int srid) => OfWithSrid(x, y, z, srid);

        /// <summary>
        ///     Creates an empty point with an explicit SRID, as the WKB reader needs it.
        /// </summary>
        internal static Point EmptyWithSrid(int srid, bool hasZ) =>
            new(srid, hasZ, true, double.NaN, double.NaN, double.NaN);

        /// <summary>
        ///     Creates a 2D point with an explicit SRID, as the WKB reader needs it.
        /// </summary>
        internal static Point OfWithSrid(double x, double y, int srid)
        {
            CoordinateValidator.ValidateX(x, srid);
            CoordinateValidator.ValidateY(y, srid);
            return new Point(srid, false, false, x, y, double.NaN);
        }

        /// <summary>
        ///     Creates a 3D point with an explicit SRID, as the WKB reader needs it.
        /// </summary>
        internal static Point OfWithSrid(double x, double y, double z, int srid)
        {
            CoordinateValidator.ValidateX(x, srid);
            CoordinateValidator.ValidateY(y, srid);
            CoordinateValidator.ValidateZ(z);
            return new Point(srid, true, false, x, y, z);
        }

        /// <summary>
        ///     Gets the x coordinate, the longitude under the geographic model.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when this point is empty.</exception>
        public double X => _empty ? throw EmptyAccess(nameof(X)) : _x;

        /// <summary>
        ///     Gets the y coordinate, the latitude under the geographic model.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when this point is empty.</exception>
        public double Y => _empty ? throw EmptyAccess(nameof(Y)) : _y;

        /// <summary>
        ///     Gets the z coordinate, or null for a 2D point.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when this point is empty.</exception>
        public double? Z => _empty ? throw EmptyAccess(nameof(Z)) : HasZ ? _z : null;

        /// <inheritdoc />
        public override bool IsEmpty => _empty;

        /// <inheritdoc />
        public override bool Equals(Geometry? other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (other is not Point point) return false;
            if (Srid != point.Srid) return false;
            if (HasZ != point.HasZ) return false;
            if (_empty != point._empty) return false;
            if (_empty) return true;
            if (_x != point._x) return false;
            if (_y != point._y) return false;
            return !HasZ || _z == point._z;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(WkbType);
            hash.Add(Srid);
            hash.Add(HasZ);
            if (!_empty)
            {
                hash.Add(CoordinateValidator.NormalizeZero(_x));
                hash.Add(CoordinateValidator.NormalizeZero(_y));
                if (HasZ)
                {
                    hash.Add(CoordinateValidator.NormalizeZero(_z));
                }
            }

            return hash.ToHashCode();
        }

        /// <inheritdoc />
        internal override int WkbTypeCode => WkbType;

        /// <inheritdoc />
        internal override bool HasNoParts => _empty;

        /// <inheritdoc />
        internal override string WktTypeName => "POINT";

        /// <inheritdoc />
        internal override void WriteWktBody(StringBuilder builder)
        {
            if (_empty)
            {
                return;
            }

            builder.Append(WktWriter.FormatDouble(_x));
            builder.Append(' ');
            builder.Append(WktWriter.FormatDouble(_y));
            if (HasZ)
            {
                builder.Append(' ');
                builder.Append(WktWriter.FormatDouble(_z));
            }
        }

        private static InvalidOperationException EmptyAccess(string name) =>
            new($"Cannot get {name} from an empty point");
    }
}
