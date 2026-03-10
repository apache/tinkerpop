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

namespace Gremlin.Net.Structure.IO.GraphBinary4
{
    /// <summary>
    /// Represents a GraphBinary 4.0 data type.
    /// </summary>
    public class DataType : IEquatable<DataType>
    {
#pragma warning disable 1591
        public static readonly DataType Int = new DataType(0x01);
        public static readonly DataType Long = new DataType(0x02);
        public static readonly DataType String = new DataType(0x03);
        public static readonly DataType DateTime = new DataType(0x04);
        public static readonly DataType Double = new DataType(0x07);
        public static readonly DataType Float = new DataType(0x08);
        public static readonly DataType List = new DataType(0x09);
        public static readonly DataType Map = new DataType(0x0A);
        public static readonly DataType Set = new DataType(0x0B);
        public static readonly DataType Uuid = new DataType(0x0C);
        public static readonly DataType Edge = new DataType(0x0D);
        public static readonly DataType Path = new DataType(0x0E);
        public static readonly DataType Property = new DataType(0x0F);
        // Not yet implemented
        // public static readonly DataType Graph = new DataType(0x10);
        public static readonly DataType Vertex = new DataType(0x11);
        public static readonly DataType VertexProperty = new DataType(0x12);
        public static readonly DataType Direction = new DataType(0x18);
        public static readonly DataType T = new DataType(0x20);
        public static readonly DataType BigDecimal = new DataType(0x22);
        public static readonly DataType BigInteger = new DataType(0x23);
        public static readonly DataType Byte = new DataType(0x24);
        public static readonly DataType Binary = new DataType(0x25);
        public static readonly DataType Short = new DataType(0x26);
        public static readonly DataType Boolean = new DataType(0x27);
        // Not yet implemented
        // public static readonly DataType Tree = new DataType(0x2B);
        public static readonly DataType Merge = new DataType(0x2E);
        // Not yet implemented
        // public static readonly DataType CompositePDT = new DataType(0xF0);
        // public static readonly DataType PrimitivePDT = new DataType(0xF1);
        public static readonly DataType Char = new DataType(0x80);
        public static readonly DataType Duration = new DataType(0x81);
        public static readonly DataType Marker = new DataType(0xFD);
#pragma warning restore 1591

        /// <summary>
        /// A null value for an unspecified Object value.
        /// </summary>
        public static readonly DataType UnspecifiedNull = new DataType(0xFE);

        private DataType(int code)
        {
            TypeCode = (byte) code;
        }
        
        /// <summary>
        ///     Gets the type code of this data type.
        /// </summary>
        public byte TypeCode { get; }

        /// <summary>
        /// Creates a new <see cref="DataType"/> instance for the given type code.
        /// </summary>
        public static DataType FromTypeCode(int code)
        {
            return new DataType(code);
        }

        /// <inheritdoc />
        public bool Equals(DataType? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return TypeCode == other.TypeCode;
        }

        /// <inheritdoc />
        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((DataType) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return TypeCode.GetHashCode();
        }

        /// <summary>
        /// Determines whether two specified <see cref="DataType"/> have the same values.
        /// </summary>
        public static bool operator ==(DataType? first, DataType? second)
        {
            if (ReferenceEquals(null, first))
            {
                return ReferenceEquals(null, second);
            }

            return first.Equals(second);
        }

        /// <summary>
        /// Determines whether two specified <see cref="DataType"/> have different values.
        /// </summary>
        public static bool operator !=(DataType? first, DataType? second)
        {
            return !(first == second);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"DataType{{ TypeCode = {TypeCode} }}";
        }
    }
}
