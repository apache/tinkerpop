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

using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Gremlin.Net.IntegrationTest.Gherkin;

public static class DeepEqualityExtensions
{
    public static bool DeepEqual(this Dictionary<object, object> first, Dictionary<object, object> second)
    {
        if (first.Count != second.Count) return false;
            
        foreach (var (key1, value1) in first)
        {
            var foundMatch = false;
            foreach (var (key2, value2) in second)
            {
                if (!key1.DeepEqual(key2) || !value1.DeepEqual(value2)) continue;
                foundMatch = true;
                break;
            }
            if (!foundMatch) return false;
        }

        return true;
    }

    private static bool DeepEqual(this object first, object second)
    {
        if (first == null)
        {
            if (second != null) return false;
        }
        else if (first is not string && first is IEnumerable enumerable1)
        {
            if (second is string || second is not IEnumerable enumerable2) return false;
            if (!enumerable1.DeepEqual(enumerable2))
            {
                return false;
            }
        }
        else
        {
            if (!first.Equals(second)) return false;
        }

        return true;
    }
    
    private static bool DeepEqual(this IEnumerable first, IEnumerable second)
    {
        if (first is Dictionary<object, object> dict1)
        {
            return second is Dictionary<object, object> dict2 && dict1.DeepEqual(dict2);
        }
        var objectEnum1 = first.ToObjectEnumerable();
        var objectEnum2 = second.ToObjectEnumerable();
            
        // I hope that these IEnumerable<object> objects will always be simple collections so we don't need to go
        //  even deeper...
        return objectEnum1.SequenceEqual(objectEnum2);
    }
    
    private static IEnumerable<object> ToObjectEnumerable(this IEnumerable enumerable)
    {
        if (enumerable.GetType().IsArray)
        {
            return (IEnumerable<object>)enumerable;
        }

        return (IEnumerable<object>)enumerable;
    }
}