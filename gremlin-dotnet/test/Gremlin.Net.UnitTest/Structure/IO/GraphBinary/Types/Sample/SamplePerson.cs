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

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary.Types.Sample
{
    public class SamplePerson : IEquatable<SamplePerson>
    {
        public SamplePerson(string name, DateTimeOffset birthDate)
        {
            Name = name;
            BirthDate = birthDate;
        }

        public string Name { get; }
        public DateTimeOffset BirthDate { get; }

        public bool Equals(SamplePerson other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Name == other.Name && BirthDate.Equals(other.BirthDate);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SamplePerson)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, BirthDate);
        }
    }
}