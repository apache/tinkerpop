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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Structure.IO.GraphBinary4;

namespace Gremlin.Net.Structure.Geo
{
    /// <summary>
    ///     Reads one ISO WKB geometry from a stream, consuming exactly the bytes of that geometry, so the stream is
    ///     left on the byte after it. EWKB is rejected.
    /// </summary>
    /// <remarks>
    ///     Each geometry header carries its own byte order marker, and the bytes of a little-endian geometry are
    ///     swapped as they are read. A stream that ends inside a geometry raises an <see cref="IOException"/> and
    ///     malformed bytes raise an <see cref="ArgumentException"/>.
    ///     <para>
    ///         A stream does not report how many bytes are left, so a count read from the input never sizes a list
    ///         beyond <see cref="MaxInitialCapacity"/>. A hostile count then ends in an unexpected end of input
    ///         once the real bytes are consumed.
    ///     </para>
    ///     <para>
    ///         A point whose x and y are NaN reads as an empty point, so the z ordinate of a 3D point is read and
    ///         discarded.
    ///     </para>
    /// </remarks>
    internal sealed class WkbReader
    {
        private const int TypePoint = 1;
        private const int TypeLineString = 2;
        private const int TypePolygon = 3;
        private const int TypeMultiPoint = 4;
        private const int TypeMultiLineString = 5;
        private const int TypeMultiPolygon = 6;
        private const int TypeGeometryCollection = 7;

        private const int EwkbZFlag = unchecked((int) 0x80000000);
        private const int EwkbMFlag = 0x40000000;
        private const int EwkbSridFlag = 0x20000000;

        /// <summary>
        ///     The largest list capacity reserved up front for a count read from the input.
        /// </summary>
        private const int MaxInitialCapacity = 1024;

        private readonly int _srid;
        private bool? _hasZ;
        private int _depth;

        private WkbReader(int srid)
        {
            _srid = srid;
        }

        /// <summary>
        ///     Reads one ISO WKB geometry from the given stream. The SRID is applied as given and not validated.
        /// </summary>
        internal static async ValueTask<Geometry> ReadAsync(Stream stream, int srid,
            CancellationToken cancellationToken = default)
        {
            return await new WkbReader(srid).ReadGeometryAsync(stream, cancellationToken).ConfigureAwait(false);
        }

        private async ValueTask<Geometry> ReadGeometryAsync(Stream stream, CancellationToken cancellationToken)
        {
            var byteOrder = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bool littleEndian;
            if (byteOrder == 0x00)
            {
                littleEndian = false;
            }
            else if (byteOrder == 0x01)
            {
                littleEndian = true;
            }
            else
            {
                throw new ArgumentException($"Invalid WKB byte order: {byteOrder}");
            }

            var typeCode = await ReadInt32Async(stream, littleEndian, cancellationToken).ConfigureAwait(false);

            if ((typeCode & EwkbZFlag) != 0 || (typeCode & EwkbMFlag) != 0 || (typeCode & EwkbSridFlag) != 0)
            {
                throw new ArgumentException($"EWKB format detected (type code 0x{typeCode:X8});"
                                            + " ISO WKB is expected. EWKB with Z, M, or SRID flags is not supported.");
            }

            if (typeCode >= 2001 && typeCode <= 2007)
            {
                throw new ArgumentException($"M dimension not supported (type code {typeCode})");
            }

            if (typeCode >= 3001 && typeCode <= 3007)
            {
                throw new ArgumentException($"ZM dimension not supported (type code {typeCode})");
            }

            int baseType;
            bool hasZ;
            if (typeCode >= 1001 && typeCode <= 1007)
            {
                baseType = typeCode - 1000;
                hasZ = true;
            }
            else if (typeCode >= 1 && typeCode <= 7)
            {
                baseType = typeCode;
                hasZ = false;
            }
            else
            {
                throw new ArgumentException($"Unknown WKB type code: {typeCode}");
            }

            if (_hasZ == null)
            {
                _hasZ = hasZ;
            }
            else if (_hasZ != hasZ)
            {
                throw new ArgumentException("Mixed Z dimensions in WKB: some geometries have Z, some do not");
            }

            switch (baseType)
            {
                case TypePoint:
                    return await ReadPointAsync(stream, hasZ, littleEndian, cancellationToken)
                        .ConfigureAwait(false);
                case TypeLineString:
                    return await ReadLineStringAsync(stream, hasZ, littleEndian, cancellationToken)
                        .ConfigureAwait(false);
                case TypePolygon:
                    return await ReadPolygonAsync(stream, hasZ, littleEndian, cancellationToken)
                        .ConfigureAwait(false);
                case TypeMultiPoint:
                    return await ReadMultiPointAsync(stream, hasZ, littleEndian, cancellationToken)
                        .ConfigureAwait(false);
                case TypeMultiLineString:
                    return await ReadMultiLineStringAsync(stream, hasZ, littleEndian, cancellationToken)
                        .ConfigureAwait(false);
                case TypeMultiPolygon:
                    return await ReadMultiPolygonAsync(stream, hasZ, littleEndian, cancellationToken)
                        .ConfigureAwait(false);
                case TypeGeometryCollection:
                    return await ReadGeometryCollectionAsync(stream, hasZ, littleEndian, cancellationToken)
                        .ConfigureAwait(false);
                default:
                    throw new ArgumentException($"Unknown WKB type: {baseType}");
            }
        }

        private async ValueTask<Point> ReadPointAsync(Stream stream, bool hasZ, bool littleEndian,
            CancellationToken cancellationToken)
        {
            var x = await ReadDoubleAsync(stream, littleEndian, cancellationToken).ConfigureAwait(false);
            var y = await ReadDoubleAsync(stream, littleEndian, cancellationToken).ConfigureAwait(false);

            if (double.IsNaN(x) && double.IsNaN(y))
            {
                if (hasZ)
                {
                    await ReadDoubleAsync(stream, littleEndian, cancellationToken).ConfigureAwait(false);
                }

                return Point.EmptyWithSrid(_srid, hasZ);
            }

            CoordinateValidator.ValidateX(x, _srid);
            CoordinateValidator.ValidateY(y, _srid);

            if (!hasZ)
            {
                return Point.OfWithSrid(x, y, _srid);
            }

            var z = await ReadDoubleAsync(stream, littleEndian, cancellationToken).ConfigureAwait(false);
            return Point.OfWithSrid(x, y, z, _srid);
        }

        /// <summary>
        ///     Reads one vertex of a line string or a ring, where NaN coordinates are malformed rather than an empty
        ///     point.
        /// </summary>
        private async ValueTask<Point> ReadVertexAsync(Stream stream, bool hasZ, bool littleEndian, string context,
            CancellationToken cancellationToken)
        {
            var x = await ReadDoubleAsync(stream, littleEndian, cancellationToken).ConfigureAwait(false);
            var y = await ReadDoubleAsync(stream, littleEndian, cancellationToken).ConfigureAwait(false);
            if (double.IsNaN(x) && double.IsNaN(y))
            {
                throw new ArgumentException($"A {context} cannot be an empty point;"
                                            + " a vertex must be a real coordinate");
            }

            CoordinateValidator.ValidateX(x, _srid);
            CoordinateValidator.ValidateY(y, _srid);

            if (!hasZ)
            {
                return Point.OfWithSrid(x, y, _srid);
            }

            var z = await ReadDoubleAsync(stream, littleEndian, cancellationToken).ConfigureAwait(false);
            return Point.OfWithSrid(x, y, z, _srid);
        }

        private async ValueTask<LineString> ReadLineStringAsync(Stream stream, bool hasZ, bool littleEndian,
            CancellationToken cancellationToken)
        {
            var pointCount = await ReadCountAsync(stream, littleEndian, "point count", cancellationToken)
                .ConfigureAwait(false);

            if (pointCount == 0)
            {
                return LineString.EmptyWithSrid(_srid, hasZ);
            }

            if (pointCount == 1)
            {
                throw new ArgumentException("LineString must have at least 2 points, got 1");
            }

            var points = new List<Point>(InitialCapacity(pointCount));
            for (var index = 0; index < pointCount; index++)
            {
                points.Add(await ReadVertexAsync(stream, hasZ, littleEndian, "linestring vertex", cancellationToken)
                    .ConfigureAwait(false));
            }

            return LineString.OfWithSrid(points, _srid, hasZ);
        }

        private async ValueTask<Polygon> ReadPolygonAsync(Stream stream, bool hasZ, bool littleEndian,
            CancellationToken cancellationToken)
        {
            var ringCount = await ReadCountAsync(stream, littleEndian, "ring count", cancellationToken)
                .ConfigureAwait(false);

            if (ringCount == 0)
            {
                return Polygon.EmptyWithSrid(_srid, hasZ);
            }

            var rings = new List<LineString>(InitialCapacity(ringCount));
            for (var ringIndex = 0; ringIndex < ringCount; ringIndex++)
            {
                var pointCount = await ReadCountAsync(stream, littleEndian, "point count for ring", cancellationToken)
                    .ConfigureAwait(false);

                if (pointCount < 4)
                {
                    throw new ArgumentException($"Polygon ring must have at least 4 points, got {pointCount}");
                }

                var points = new List<Point>(InitialCapacity(pointCount));
                for (var pointIndex = 0; pointIndex < pointCount; pointIndex++)
                {
                    points.Add(await ReadVertexAsync(stream, hasZ, littleEndian, "ring vertex", cancellationToken)
                        .ConfigureAwait(false));
                }

                var ring = LineString.OfWithSrid(points, _srid, hasZ);
                CoordinateValidator.ValidateRing(ring, ringIndex == 0 ? "Shell ring" : "Hole ring");
                rings.Add(ring);
            }

            var shell = rings[0];
            var holes = rings.GetRange(1, rings.Count - 1);
            return Polygon.OfWithSrid(shell, holes, _srid, hasZ);
        }

        private async ValueTask<MultiPoint> ReadMultiPointAsync(Stream stream, bool hasZ, bool littleEndian,
            CancellationToken cancellationToken)
        {
            _depth++;
            CoordinateValidator.ValidateNestingDepth(_depth);

            var geometryCount = await ReadCountAsync(stream, littleEndian, "geometry count", cancellationToken)
                .ConfigureAwait(false);

            if (geometryCount == 0)
            {
                _depth--;
                return MultiPoint.EmptyWithSrid(_srid, hasZ);
            }

            var points = new List<Point>(InitialCapacity(geometryCount));
            for (var index = 0; index < geometryCount; index++)
            {
                var geometry = await ReadGeometryAsync(stream, cancellationToken).ConfigureAwait(false);
                if (geometry is not Point point)
                {
                    throw new ArgumentException(
                        $"Expected Point in MultiPoint, got {geometry.GetType().Name}");
                }

                points.Add(point);
            }

            _depth--;
            return MultiPoint.OfWithSrid(points, _srid, hasZ);
        }

        private async ValueTask<MultiLineString> ReadMultiLineStringAsync(Stream stream, bool hasZ,
            bool littleEndian, CancellationToken cancellationToken)
        {
            _depth++;
            CoordinateValidator.ValidateNestingDepth(_depth);

            var geometryCount = await ReadCountAsync(stream, littleEndian, "geometry count", cancellationToken)
                .ConfigureAwait(false);

            if (geometryCount == 0)
            {
                _depth--;
                return MultiLineString.EmptyWithSrid(_srid, hasZ);
            }

            var lineStrings = new List<LineString>(InitialCapacity(geometryCount));
            for (var index = 0; index < geometryCount; index++)
            {
                var geometry = await ReadGeometryAsync(stream, cancellationToken).ConfigureAwait(false);
                if (geometry is not LineString lineString)
                {
                    throw new ArgumentException(
                        $"Expected LineString in MultiLineString, got {geometry.GetType().Name}");
                }

                lineStrings.Add(lineString);
            }

            _depth--;
            return MultiLineString.OfWithSrid(lineStrings, _srid, hasZ);
        }

        private async ValueTask<MultiPolygon> ReadMultiPolygonAsync(Stream stream, bool hasZ, bool littleEndian,
            CancellationToken cancellationToken)
        {
            _depth++;
            CoordinateValidator.ValidateNestingDepth(_depth);

            var geometryCount = await ReadCountAsync(stream, littleEndian, "geometry count", cancellationToken)
                .ConfigureAwait(false);

            if (geometryCount == 0)
            {
                _depth--;
                return MultiPolygon.EmptyWithSrid(_srid, hasZ);
            }

            var polygons = new List<Polygon>(InitialCapacity(geometryCount));
            for (var index = 0; index < geometryCount; index++)
            {
                var geometry = await ReadGeometryAsync(stream, cancellationToken).ConfigureAwait(false);
                if (geometry is not Polygon polygon)
                {
                    throw new ArgumentException(
                        $"Expected Polygon in MultiPolygon, got {geometry.GetType().Name}");
                }

                polygons.Add(polygon);
            }

            _depth--;
            return MultiPolygon.OfWithSrid(polygons, _srid, hasZ);
        }

        private async ValueTask<GeometryCollection> ReadGeometryCollectionAsync(Stream stream, bool hasZ,
            bool littleEndian, CancellationToken cancellationToken)
        {
            _depth++;
            CoordinateValidator.ValidateNestingDepth(_depth);

            var geometryCount = await ReadCountAsync(stream, littleEndian, "geometry count", cancellationToken)
                .ConfigureAwait(false);

            if (geometryCount == 0)
            {
                _depth--;
                return GeometryCollection.EmptyWithSrid(_srid, hasZ);
            }

            var geometries = new List<Geometry>(InitialCapacity(geometryCount));
            for (var index = 0; index < geometryCount; index++)
            {
                geometries.Add(await ReadGeometryAsync(stream, cancellationToken).ConfigureAwait(false));
            }

            _depth--;
            return GeometryCollection.OfWithSrid(geometries, _srid, hasZ);
        }

        /// <summary>
        ///     Reads a count of points, rings or parts. A count of 2^31 or more is rejected rather than read as a
        ///     negative number.
        /// </summary>
        private static async ValueTask<int> ReadCountAsync(Stream stream, bool littleEndian, string context,
            CancellationToken cancellationToken)
        {
            var count = await ReadInt32Async(stream, littleEndian, cancellationToken).ConfigureAwait(false);
            if (count < 0)
            {
                throw new ArgumentException($"Invalid {context}: {(uint) count}");
            }

            return count;
        }

        private static async ValueTask<int> ReadInt32Async(Stream stream, bool littleEndian,
            CancellationToken cancellationToken)
        {
            var value = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);
            return littleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
        }

        private static async ValueTask<double> ReadDoubleAsync(Stream stream, bool littleEndian,
            CancellationToken cancellationToken)
        {
            var bits = await stream.ReadLongAsync(cancellationToken).ConfigureAwait(false);
            return BitConverter.Int64BitsToDouble(littleEndian ? BinaryPrimitives.ReverseEndianness(bits) : bits);
        }

        private static int InitialCapacity(int count) => Math.Min(count, MaxInitialCapacity);
    }
}
