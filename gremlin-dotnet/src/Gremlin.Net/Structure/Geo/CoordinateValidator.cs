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

namespace Gremlin.Net.Structure.Geo
{
    /// <summary>
    ///     Validates the coordinates and the structure of a geometry, and holds the helpers its equality and hash
    ///     code are built from.
    /// </summary>
    internal static class CoordinateValidator
    {
        internal const int SridCartesian = 0;
        internal const int SridGeographic = 4326;
        internal const int MaxNestingDepth = 32;

        internal static Model ModelForSrid(int srid) => srid switch
        {
            SridCartesian => Model.Cartesian,
            SridGeographic => Model.Geographic,
            _ => Model.Other
        };

        internal static int SridForModel(Model model) => model switch
        {
            Model.Cartesian => SridCartesian,
            Model.Geographic => SridGeographic,
            Model.Other => throw new ArgumentException(
                "Model.Other cannot be used in constructors; use Geometry.FromWkb(wkb, srid) with an explicit SRID",
                nameof(model)),
            _ => throw new ArgumentException($"Unknown coordinate model: {model}", nameof(model))
        };

        internal static void ValidateX(double x, int srid)
        {
            if (double.IsNaN(x) || double.IsInfinity(x))
            {
                throw new ArgumentException("x coordinate cannot be NaN or infinite");
            }

            if (srid == SridGeographic && (x < -180.0 || x > 180.0))
            {
                throw new ArgumentException($"GEOGRAPHIC longitude must be in [-180, 180], got: {x}");
            }
        }

        internal static void ValidateY(double y, int srid)
        {
            if (double.IsNaN(y) || double.IsInfinity(y))
            {
                throw new ArgumentException("y coordinate cannot be NaN or infinite");
            }

            if (srid == SridGeographic && (y < -90.0 || y > 90.0))
            {
                throw new ArgumentException($"GEOGRAPHIC latitude must be in [-90, 90], got: {y}");
            }
        }

        internal static void ValidateZ(double z)
        {
            if (double.IsNaN(z))
            {
                throw new ArgumentException(
                    "z coordinate cannot be NaN when provided; use a 2D point for a point without z");
            }

            if (double.IsInfinity(z))
            {
                throw new ArgumentException("z coordinate cannot be infinite");
            }
        }

        internal static void ValidateNestingDepth(int depth)
        {
            if (depth > MaxNestingDepth)
            {
                throw new ArgumentException($"Collection nesting depth exceeds maximum of {MaxNestingDepth}");
            }
        }

        /// <summary>
        ///     Returns the collection nesting depth of a geometry. Every collection, including a multi-point, a
        ///     multi-line string and a multi-polygon, counts as one level. Any other kind has depth 0.
        /// </summary>
        internal static int ComputeDepth(Geometry geometry)
        {
            if (geometry is not GeometryCollection collection)
            {
                return 0;
            }

            var deepestChild = 0;
            foreach (var child in collection.Geometries)
            {
                deepestChild = Math.Max(deepestChild, ComputeDepth(child));
            }

            return 1 + deepestChild;
        }

        internal static void ValidateSameSrid(int expected, int actual, string context)
        {
            if (expected != actual)
            {
                throw new ArgumentException($"Mixed SRIDs in {context}: expected {expected}, got {actual}");
            }
        }

        internal static void ValidateSameHasZ(bool expected, bool actual, string context)
        {
            if (expected != actual)
            {
                throw new ArgumentException(
                    $"Mixed z dimensions in {context}: some coordinates have z, some do not");
            }
        }

        /// <summary>
        ///     Validates a polygon ring, which must be closed and hold at least 4 points. An empty ring is only
        ///     valid as the whole polygon being empty.
        /// </summary>
        internal static void ValidateRing(LineString ring, string context)
        {
            if (ring.IsEmpty)
            {
                return;
            }

            var points = ring.Points;
            if (points.Count < 4)
            {
                throw new ArgumentException($"{context} must have at least 4 points, got: {points.Count}");
            }

            if (!points[0].Equals(points[points.Count - 1]))
            {
                throw new ArgumentException($"{context} must be closed (first and last point must be equal)");
            }
        }

        /// <summary>
        ///     Validates that a point can be a vertex of a line string or a polygon ring. An empty point is
        ///     rejected.
        /// </summary>
        internal static void ValidateVertex(Point point, string context, int index)
        {
            RequireElement(point, context, index);
            if (point.IsEmpty)
            {
                throw new ArgumentException($"{context} vertex at index {index} cannot be an empty point;"
                                            + " a vertex must be a real coordinate");
            }
        }

        internal static void RequireCollection(object? collection, string parameterName)
        {
            if (collection == null)
            {
                throw new ArgumentNullException(parameterName);
            }
        }

        internal static void RequireElement(object? element, string context, int index)
        {
            if (element == null)
            {
                throw new ArgumentNullException(context, $"{context} element at index {index} cannot be null");
            }
        }

        /// <summary>
        ///     Returns a coordinate with its zero normalized, so that -0.0 and 0.0 hash alike.
        /// </summary>
        internal static double NormalizeZero(double value) => value == 0.0 ? 0.0 : value;

        /// <summary>
        ///     Determines whether two sequences of parts hold equal parts in the same order.
        /// </summary>
        internal static bool PartsEqual<TPart>(IReadOnlyList<TPart> first, IReadOnlyList<TPart> second)
            where TPart : Geometry
        {
            if (first.Count != second.Count)
            {
                return false;
            }

            for (var index = 0; index < first.Count; index++)
            {
                if (!first[index].Equals(second[index]))
                {
                    return false;
                }
            }

            return true;
        }

        internal static int PartsHashCode<TPart>(int wkbTypeCode, int srid, bool hasZ, IReadOnlyList<TPart> parts)
            where TPart : Geometry
        {
            var hash = new HashCode();
            hash.Add(wkbTypeCode);
            hash.Add(srid);
            hash.Add(hasZ);
            foreach (var part in parts)
            {
                hash.Add(part);
            }

            return hash.ToHashCode();
        }
    }
}
