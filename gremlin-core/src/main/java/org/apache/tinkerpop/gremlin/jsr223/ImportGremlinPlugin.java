/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.jsr223;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A module that allows custom class, static method and enum imports (i.e. those that are statically defined by a
 * module within itself). A user might utilize this class to supply their own imports. This module is not specific
 * to any {@link GremlinScriptEngine} - the imports are supplied to all engines.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ImportGremlinPlugin extends AbstractGremlinPlugin {
    private static final String MODULE_NAME = "tinkerpop.import";

    private ImportGremlinPlugin(final Builder builder) {
        super(MODULE_NAME, builder.appliesTo, DefaultImportCustomizer.build()
                                                .addClassImports(builder.classImports)
                                                .addEnumImports(builder.enumImports)
                                                .addMethodImports(builder.methodImports).create());
    }

    public static Builder build() {
        return new Builder();
    }

    public static final class Builder {
        private static final Pattern METHOD_PATTERN = Pattern.compile("(.*)#(.*)\\((.*)\\)");
        private static final Pattern ENUM_PATTERN = Pattern.compile("(.*)#(.*)");
        private Set<Class> classImports = new HashSet<>();
        private Set<Method> methodImports = new HashSet<>();
        private Set<Enum> enumImports = new HashSet<>();
        private final Set<String> appliesTo = new HashSet<>();

        private Builder() {}

        /**
         * The name of the {@link GremlinScriptEngine} that this module will apply to. Setting no values here will
         * make the module available to all the engines.
         */
        public Builder appliesTo(final Collection<String> scriptEngineName) {
            this.appliesTo.addAll(scriptEngineName);
            return this;
        }

        public Builder classImports(final Collection<String> classes) {
            for (String clazz : classes) {
                try {
                    classImports.add(Class.forName(clazz));
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return this;
        }

        public Builder methodImports(final Collection<String> methods) {
            for (String method : methods) {
                try {
                    if (method.endsWith("#*")) {
                        final String classString = method.substring(0, method.length() - 2);
                        final Class<?> clazz = Class.forName(classString);
                        methodImports.addAll(allStaticMethods(clazz));
                    } else {
                        final Matcher matcher = METHOD_PATTERN.matcher(method);

                        if (!matcher.matches())
                            throw new IllegalArgumentException(String.format("Could not read method descriptor - check format of: %s", method));

                        final String classString = matcher.group(1);
                        final String methodString = matcher.group(2);
                        final String argString = matcher.group(3);
                        final Class<?> clazz = Class.forName(classString);
                        methodImports.add(clazz.getMethod(methodString, parse(argString)));
                    }
                } catch (IllegalArgumentException iae) {
                    throw iae;
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return this;
        }

        public Builder enumImports(final Collection<String> enums) {
            for (String enumItem : enums) {
                try {
                    if (enumItem.endsWith("#*")) {
                        final String classString = enumItem.substring(0, enumItem.length() - 2);
                        final Class<?> clazz = Class.forName(classString);
                        enumImports.addAll(allEnums(clazz));
                    } else {
                        final Matcher matcher = ENUM_PATTERN.matcher(enumItem);

                        if (!matcher.matches())
                            throw new IllegalArgumentException(String.format("Could not read enum descriptor - check format of: %s", enumItem));

                        final String classString = matcher.group(1);
                        final String enumValString = matcher.group(2);
                        final Class<?> clazz = Class.forName(classString);

                        Stream.of(clazz.getEnumConstants())
                                .filter(e -> ((Enum) e).name().equals(enumValString))
                                .findFirst().ifPresent(e -> enumImports.add((Enum) e));
                    }
                } catch (IllegalArgumentException iae) {
                    throw iae;
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return this;
        }

        public ImportGremlinPlugin create() {
            if (enumImports.isEmpty() && classImports.isEmpty() && methodImports.isEmpty())
                throw new IllegalStateException("At least one import must be specified");

            return new ImportGremlinPlugin(this);
        }

        private static List<Enum> allEnums(final Class<?> clazz) {
            return Stream.of(clazz.getEnumConstants()).map(e -> (Enum) e).collect(Collectors.toList());
        }

        private static List<Method> allStaticMethods(final Class<?> clazz) {
            return Stream.of(clazz.getMethods()).filter(m -> Modifier.isStatic(m.getModifiers())).collect(Collectors.toList());
        }

        private static Class<?>[] parse(final String argString) {
            if (null == argString || argString.isEmpty())
                return new Class<?>[0];

            final List<String> args = Stream.of(argString.split(",")).map(String::trim).collect(Collectors.toList());
            final Class<?>[] classes = new Class<?>[args.size()];
            for (int ix = 0; ix < args.size(); ix++) {
                try {
                    classes[ix] = Class.forName(args.get(ix));
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }

            return classes;
        }
    }
}
