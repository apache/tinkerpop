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
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;

namespace Gremlin.Net.Structure
{
    /// <summary>
    /// Registry for <see cref="IProviderDefinedTypeAdapter{T}"/> instances that hydrate
    /// <see cref="ProviderDefinedType"/> values into strongly-typed objects.
    /// </summary>
    public class ProviderDefinedTypeRegistry
    {
        private readonly Dictionary<string, object> _adaptersByName = new();
        private readonly Dictionary<Type, (string typeName, object adapter)> _adaptersByType = new();

        /// <summary>
        /// Registers an adapter for a specific provider-defined type name.
        /// </summary>
        public void Register<T>(IProviderDefinedTypeAdapter<T> adapter)
        {
            _adaptersByName[adapter.TypeName] = adapter;
            _adaptersByType[typeof(T)] = (adapter.TypeName, adapter);
        }

        /// <summary>
        /// Creates a registry populated by scanning loaded assemblies for:
        /// <list type="bullet">
        /// <item>Types implementing <see cref="IProviderDefinedTypeAdapter{T}"/> (adapter-based hydration)</item>
        /// <item>Types annotated with <see cref="ProviderDefinedAttribute"/> (annotation-based round-trip)</item>
        /// </list>
        /// </summary>
        public static ProviderDefinedTypeRegistry Build()
        {
            var registry = new ProviderDefinedTypeRegistry();

            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    foreach (var type in assembly.GetTypes())
                    {
                        // Register adapter implementations
                        var adapterInterface = type.GetInterfaces()
                            .FirstOrDefault(i => i.IsGenericType &&
                                i.GetGenericTypeDefinition() == typeof(IProviderDefinedTypeAdapter<>));
                        if (adapterInterface != null && !type.IsAbstract && !type.IsInterface)
                        {
                            try
                            {
                                var adapter = Activator.CreateInstance(type);
                                var registerMethod = typeof(ProviderDefinedTypeRegistry)
                                    .GetMethod(nameof(Register))!
                                    .MakeGenericMethod(adapterInterface.GetGenericArguments()[0]);
                                registerMethod.Invoke(registry, new[] { adapter });
                            }
                            catch
                            {
                                // skip types that can't be instantiated
                            }
                        }

                        // Register annotated types for annotation-based round-trip hydration
                        var pdtAttr = type.GetCustomAttribute<ProviderDefinedAttribute>();
                        if (pdtAttr != null)
                        {
                            var typeName = !string.IsNullOrEmpty(pdtAttr.Name) ? pdtAttr.Name : type.Name;
                            ProviderDefinedAttribute.RegisteredTypes.TryAdd(typeName, type);
                        }
                    }
                }
                catch
                {
                    // skip assemblies that can't be reflected
                }
            }

            return registry;
        }

        /// <summary>
        /// Returns the type name and ToProperties method for the given CLR type, or null if not registered.
        /// </summary>
        internal (string typeName, Func<object, IReadOnlyDictionary<string, object?>>)? GetAdapterByType(Type type)
        {
            if (!_adaptersByType.TryGetValue(type, out var entry))
                return null;
            var method = entry.adapter.GetType().GetMethod("ToProperties");
            if (method == null) return null;
            return (entry.typeName, obj => (IReadOnlyDictionary<string, object?>)method.Invoke(entry.adapter, new[] { obj })!);
        }

        /// <summary>
        /// Hydrates a <see cref="ProviderDefinedType"/> into a typed object using a registered adapter.
        /// Returns the original PDT if no adapter is registered or if hydration fails.
        /// </summary>
        public object Hydrate(ProviderDefinedType pdt)
        {
            if (!_adaptersByName.TryGetValue(pdt.Name, out var adapterObj))
                return pdt;
            try
            {
                var hydratedProps = new Dictionary<string, object?>();
                foreach (var (key, value) in pdt.Properties)
                {
                    hydratedProps[key] = value is ProviderDefinedType nested ? Hydrate(nested) : value;
                }

                var readOnlyProps = new ReadOnlyDictionary<string, object?>(hydratedProps);
                var method = adapterObj.GetType().GetMethod("FromProperties");
                return method!.Invoke(adapterObj, new object[] { readOnlyProps })!;
            }
            catch (Exception)
            {
                return pdt;
            }
        }
    }
}
