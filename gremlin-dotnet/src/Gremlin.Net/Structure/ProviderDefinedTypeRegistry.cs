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
    /// Registry for <see cref="IProviderDefinedTypeAdapter{T}"/> and <see cref="IPrimitivePdtAdapter{T}"/>
    /// instances that hydrate provider-defined types into strongly-typed objects.
    /// </summary>
    public class ProviderDefinedTypeRegistry
    {
        private readonly Dictionary<string, object> _compositeAdaptersByName = new();
        private readonly Dictionary<Type, (string typeName, object adapter)> _compositeAdaptersByType = new();
        private readonly Dictionary<string, object> _primitiveAdaptersByName = new();
        private readonly Dictionary<Type, (string typeName, object adapter)> _primitiveAdaptersByType = new();

        /// <summary>
        /// Registers a composite adapter for a specific provider-defined type name.
        /// </summary>
        public void Register<T>(IProviderDefinedTypeAdapter<T> adapter)
        {
            _compositeAdaptersByName[adapter.TypeName] = adapter;
            _compositeAdaptersByType[typeof(T)] = (adapter.TypeName, adapter);
        }

        /// <summary>
        /// Registers a primitive adapter for a specific provider-defined type name.
        /// </summary>
        public void RegisterPrimitive<T>(IPrimitivePdtAdapter<T> adapter)
        {
            _primitiveAdaptersByName[adapter.TypeName] = adapter;
            _primitiveAdaptersByType[typeof(T)] = (adapter.TypeName, adapter);
        }

        /// <summary>
        /// Creates a registry populated by scanning loaded assemblies for:
        /// <list type="bullet">
        /// <item>Types implementing <see cref="IProviderDefinedTypeAdapter{T}"/> (composite adapter-based hydration)</item>
        /// <item>Types implementing <see cref="IPrimitivePdtAdapter{T}"/> (primitive adapter-based hydration)</item>
        /// <item>Types annotated with <see cref="ProviderDefinedAttribute"/> (annotation-based round-trip)</item>
        /// </list>
        /// </summary>
        public static ProviderDefinedTypeRegistry Create()
        {
            var registry = new ProviderDefinedTypeRegistry();

            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    foreach (var type in assembly.GetTypes())
                    {
                        // Register composite adapter implementations
                        var compositeInterface = type.GetInterfaces()
                            .FirstOrDefault(i => i.IsGenericType &&
                                i.GetGenericTypeDefinition() == typeof(IProviderDefinedTypeAdapter<>));
                        if (compositeInterface != null && !type.IsAbstract && !type.IsInterface)
                        {
                            try
                            {
                                var adapter = Activator.CreateInstance(type);
                                var registerMethod = typeof(ProviderDefinedTypeRegistry)
                                    .GetMethod(nameof(Register))!
                                    .MakeGenericMethod(compositeInterface.GetGenericArguments()[0]);
                                registerMethod.Invoke(registry, new[] { adapter });
                            }
                            catch
                            {
                                // skip types that can't be instantiated
                            }
                        }

                        // Register primitive adapter implementations
                        var primitiveInterface = type.GetInterfaces()
                            .FirstOrDefault(i => i.IsGenericType &&
                                i.GetGenericTypeDefinition() == typeof(IPrimitivePdtAdapter<>));
                        if (primitiveInterface != null && !type.IsAbstract && !type.IsInterface)
                        {
                            try
                            {
                                var adapter = Activator.CreateInstance(type);
                                var registerMethod = typeof(ProviderDefinedTypeRegistry)
                                    .GetMethod(nameof(RegisterPrimitive))!
                                    .MakeGenericMethod(primitiveInterface.GetGenericArguments()[0]);
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
        /// Returns the type name and ToFields method for the given CLR type, or null if not registered as composite.
        /// </summary>
        internal (string typeName, Func<object, IReadOnlyDictionary<string, object?>>)? GetCompositeAdapterByType(Type type)
        {
            if (!_compositeAdaptersByType.TryGetValue(type, out var entry))
                return null;
            var method = entry.adapter.GetType().GetMethod("ToFields");
            if (method == null) return null;
            return (entry.typeName, obj => (IReadOnlyDictionary<string, object?>)method.Invoke(entry.adapter, new[] { obj })!);
        }

        /// <summary>
        /// Returns the type name and ToString method for the given CLR type, or null if not registered as primitive.
        /// </summary>
        internal (string typeName, Func<object, string>)? GetPrimitiveAdapterByType(Type type)
        {
            if (!_primitiveAdaptersByType.TryGetValue(type, out var entry))
                return null;
            var method = entry.adapter.GetType().GetMethod("ToString", new[] { type });
            if (method == null) return null;
            return (entry.typeName, obj => (string)method.Invoke(entry.adapter, new[] { obj })!);
        }

        /// <summary>
        /// Returns the type name and ToFields method for the given CLR type (composite),
        /// or type name and ToString method (primitive). Checks primitive first, then composite.
        /// Returns null if not registered.
        /// </summary>
        internal (string typeName, Func<object, IReadOnlyDictionary<string, object?>>)? GetAdapterByType(Type type)
        {
            // Check composite adapters (backward compatibility with existing callers)
            return GetCompositeAdapterByType(type);
        }

        /// <summary>
        /// Hydrates a <see cref="ProviderDefinedType"/> into a typed object using a registered composite adapter.
        /// Returns the original PDT if no adapter is registered or if hydration fails.
        /// </summary>
        public object HydrateComposite(ProviderDefinedType pdt)
        {
            if (!_compositeAdaptersByName.TryGetValue(pdt.Name, out var adapterObj))
            {
                // No adapter for outer — still recurse into nested PDT fields
                Dictionary<string, object?>? resolved = null;
                foreach (var (key, value) in pdt.Fields)
                {
                    object? hydrated = value;
                    if (value is ProviderDefinedType nested)
                        hydrated = HydrateComposite(nested);
                    else if (value is PrimitiveProviderDefinedType nestedPrim)
                        hydrated = HydratePrimitive(nestedPrim);

                    if (!ReferenceEquals(hydrated, value))
                    {
                        resolved ??= new Dictionary<string, object?>(pdt.Fields);
                        resolved[key] = hydrated;
                    }
                }
                return resolved != null ? new ProviderDefinedType(pdt.Name, resolved) : pdt;
            }
            try
            {
                var hydratedFields = new Dictionary<string, object?>();
                foreach (var (key, value) in pdt.Fields)
                {
                    if (value is ProviderDefinedType nested)
                        hydratedFields[key] = HydrateComposite(nested);
                    else if (value is PrimitiveProviderDefinedType nestedPrim)
                        hydratedFields[key] = HydratePrimitive(nestedPrim);
                    else
                        hydratedFields[key] = value;
                }

                var readOnlyFields = new ReadOnlyDictionary<string, object?>(hydratedFields);
                var method = adapterObj.GetType().GetMethod("FromFields");
                return method!.Invoke(adapterObj, new object[] { readOnlyFields })!;
            }
            catch (Exception)
            {
                return pdt;
            }
        }

        /// <summary>
        /// Hydrates a <see cref="PrimitiveProviderDefinedType"/> into a typed object using a registered primitive adapter.
        /// Returns the original primitive PDT if no adapter is registered or if hydration fails.
        /// </summary>
        public object HydratePrimitive(PrimitiveProviderDefinedType pdt)
        {
            if (!_primitiveAdaptersByName.TryGetValue(pdt.Name, out var adapterObj))
                return pdt;
            try
            {
                var method = adapterObj.GetType().GetMethod("FromString");
                return method!.Invoke(adapterObj, new object[] { pdt.Value })!;
            }
            catch (Exception)
            {
                return pdt;
            }
        }

        /// <summary>
        /// Hydrates a <see cref="ProviderDefinedType"/> into a typed object using a registered composite adapter.
        /// Returns the original PDT if no adapter is registered or if hydration fails.
        /// </summary>
        [Obsolete("Use HydrateComposite instead.")]
        public object Hydrate(ProviderDefinedType pdt) => HydrateComposite(pdt);
    }
}
