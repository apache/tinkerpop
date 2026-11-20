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

namespace Gremlin.Net.Structure.Geo
{
    /// <summary>
    ///     Writes a geometry as ISO WKB in big-endian byte order.
    /// </summary>
    internal static class WkbWriter
    {
        private const byte BigEndian = 0x00;

        /// <summary>
        ///     The quiet NaN that ISO WKB uses for the coordinates of an empty point. The <c>double.NaN</c> of this
        ///     platform carries a sign bit, so these bits are written directly.
        /// </summary>
        private const long NanBits = 0x7ff8000000000000L;

        /// <summary>
        ///     Returns the ISO WKB bytes of a geometry.
        /// </summary>
        internal static byte[] Write(Geometry geometry)
        {
            using var stream = new MemoryStream();
            WriteGeometry(stream, geometry);
            return stream.ToArray();
        }

        private static void WriteGeometry(MemoryStream stream, Geometry geometry)
        {
            stream.WriteByte(BigEndian);

            var typeCode = geometry.WkbTypeCode;
            if (geometry.HasZ)
            {
                typeCode += 1000;
            }

            WriteInt(stream, typeCode);

            switch (geometry)
            {
                case Point point:
                    WriteCoordinate(stream, point, point.HasZ);
                    break;
                case LineString lineString:
                    WriteLineString(stream, lineString);
                    break;
                case Polygon polygon:
                    WritePolygon(stream, polygon);
                    break;
                case MultiPoint multiPoint:
                    WriteParts(stream, multiPoint.Points);
                    break;
                case MultiLineString multiLineString:
                    WriteParts(stream, multiLineString.LineStrings);
                    break;
                case MultiPolygon multiPolygon:
                    WriteParts(stream, multiPolygon.Polygons);
                    break;
                case GeometryCollection collection:
                    WriteParts(stream, collection.Geometries);
                    break;
                default:
                    throw new InvalidOperationException(
                        $"No WKB encoding for geometry type {geometry.GetType().Name}");
            }
        }

        private static void WriteLineString(MemoryStream stream, LineString lineString)
        {
            WriteInt(stream, lineString.Points.Count);
            foreach (var point in lineString.Points)
            {
                WriteCoordinate(stream, point, lineString.HasZ);
            }
        }

        private static void WritePolygon(MemoryStream stream, Polygon polygon)
        {
            var ringCount = polygon.RingCount;
            WriteInt(stream, ringCount);
            if (ringCount == 0)
            {
                return;
            }

            WriteLineString(stream, polygon.ExteriorRing);
            foreach (var hole in polygon.InteriorRings)
            {
                WriteLineString(stream, hole);
            }
        }

        private static void WriteParts<TPart>(MemoryStream stream, IReadOnlyList<TPart> parts)
            where TPart : Geometry
        {
            WriteInt(stream, parts.Count);
            foreach (var part in parts)
            {
                WriteGeometry(stream, part);
            }
        }

        /// <summary>
        ///     Writes the coordinates of a point, with z when the enclosing geometry is 3D. An empty point is
        ///     encoded as NaN coordinates.
        /// </summary>
        private static void WriteCoordinate(MemoryStream stream, Point point, bool hasZ)
        {
            if (point.IsEmpty)
            {
                WriteLong(stream, NanBits);
                WriteLong(stream, NanBits);
                if (hasZ)
                {
                    WriteLong(stream, NanBits);
                }

                return;
            }

            WriteDouble(stream, point.X);
            WriteDouble(stream, point.Y);
            if (hasZ)
            {
                WriteDouble(stream, point.Z!.Value);
            }
        }

        private static void WriteInt(MemoryStream stream, int value)
        {
            Span<byte> bytes = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            stream.Write(bytes);
        }

        private static void WriteLong(MemoryStream stream, long value)
        {
            Span<byte> bytes = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(bytes, value);
            stream.Write(bytes);
        }

        private static void WriteDouble(MemoryStream stream, double value)
        {
            Span<byte> bytes = stackalloc byte[8];
            BinaryPrimitives.WriteDoubleBigEndian(bytes, value);
            stream.Write(bytes);
        }
    }
}
