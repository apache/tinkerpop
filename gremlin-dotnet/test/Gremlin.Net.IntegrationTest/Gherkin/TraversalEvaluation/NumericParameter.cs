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

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    public class NumericParameter<T> : ITokenParameter, IEquatable<NumericParameter<T>> where T : struct
    {
        public bool Equals(NumericParameter<T> other)
        {
            return Value.Equals(other.Value);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((NumericParameter<T>) obj);
        }

        public override int GetHashCode()
        {
            return Value.GetHashCode();
        }

        public T Value { get; }
        
        public NumericParameter(T value)
        {
            Value = value;
        }

        public override string ToString()
        {
            return $"NumericParameter<{typeof(T).Name}>({Value})";
        }

        public object GetValue(IDictionary<string, object> contextParameterValues)
        {
            return Value;
        }

        public Type GetParameterType()
        {
            return typeof(T);
        }
    }

    internal static class NumericParameter
    {
        public static NumericParameter<TType> Create<TType>(TType value) where TType : struct
        {
            return new NumericParameter<TType>(value);
        }

        public static NumericParameter<long> CreateLong(string value)
        {
            return NumericParameter.Create(Convert.ToInt64(value.Substring(0, value.Length - 1)));
        }
    }
}