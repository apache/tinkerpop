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
using System.Globalization;
using System.Text;

namespace Gremlin.Net.Structure.Geo
{
    /// <summary>
    ///     Writes a geometry as WKT text.
    /// </summary>
    internal static class WktWriter
    {
        /// <summary>
        ///     Returns the WKT text of a geometry.
        /// </summary>
        internal static string Write(Geometry geometry)
        {
            var builder = new StringBuilder();
            WriteTagged(builder, geometry);
            return builder.ToString();
        }

        /// <summary>
        ///     Appends a geometry with its type name, its Z tag and either EMPTY or a parenthesized body.
        /// </summary>
        internal static void WriteTagged(StringBuilder builder, Geometry geometry)
        {
            builder.Append(geometry.WktTypeName);
            if (geometry.HasZ)
            {
                builder.Append(" Z");
            }

            if (geometry.HasNoParts)
            {
                builder.Append(" EMPTY");
            }
            else
            {
                if (geometry.HasZ)
                {
                    builder.Append(' ');
                }

                builder.Append('(');
                geometry.WriteWktBody(builder);
                builder.Append(')');
            }
        }

        /// <summary>
        ///     Returns the shortest decimal text that reads back as the given coordinate, in plain notation with no
        ///     exponent and no trailing <c>.0</c>. Negative zero keeps its sign.
        /// </summary>
        internal static string FormatDouble(double value)
        {
            if (double.IsNaN(value))
            {
                throw new InvalidOperationException("Cannot format NaN in WKT");
            }

            if (double.IsInfinity(value))
            {
                throw new InvalidOperationException("Cannot format Infinity in WKT");
            }

            if (value == 0.0)
            {
                return double.IsNegative(value) ? "-0" : "0";
            }

            // The round-trip format switches to an exponent for extreme magnitudes, rewritten below as a decimal.
            var text = value.ToString("R", CultureInfo.InvariantCulture);
            var exponentIndex = text.IndexOf('E');
            return exponentIndex < 0 ? text : ToPlainDecimal(text, exponentIndex);
        }

        /// <summary>
        ///     Rewrites a decimal written with an exponent, such as <c>1.5E-07</c>, as a plain decimal.
        /// </summary>
        private static string ToPlainDecimal(string text, int exponentIndex)
        {
            var negative = text[0] == '-';
            var mantissa = text.Substring(negative ? 1 : 0, exponentIndex - (negative ? 1 : 0));
            var exponent = int.Parse(text.Substring(exponentIndex + 1), CultureInfo.InvariantCulture);

            var pointIndex = mantissa.IndexOf('.');
            var digits = pointIndex < 0 ? mantissa : mantissa.Remove(pointIndex, 1);
            // Where the decimal point lands once the exponent is applied.
            var pointPosition = (pointIndex < 0 ? mantissa.Length : pointIndex) + exponent;

            var builder = new StringBuilder();
            if (negative)
            {
                builder.Append('-');
            }

            if (pointPosition <= 0)
            {
                builder.Append("0.");
                builder.Append('0', -pointPosition);
                builder.Append(digits);
            }
            else if (pointPosition >= digits.Length)
            {
                builder.Append(digits);
                builder.Append('0', pointPosition - digits.Length);
            }
            else
            {
                builder.Append(digits, 0, pointPosition);
                builder.Append('.');
                builder.Append(digits, pointPosition, digits.Length - pointPosition);
            }

            return builder.ToString();
        }
    }
}
