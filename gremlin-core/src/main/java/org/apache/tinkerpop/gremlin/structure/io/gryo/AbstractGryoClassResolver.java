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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.shaded.kryo.ClassResolver;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.KryoException;
import org.apache.tinkerpop.shaded.kryo.Registration;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.util.IdentityObjectIntMap;
import org.apache.tinkerpop.shaded.kryo.util.IntMap;
import org.apache.tinkerpop.shaded.kryo.util.ObjectMap;

import static org.apache.tinkerpop.shaded.kryo.util.Util.getWrapperClass;

/**
 * This mapper implementation of the {@code ClassResolver} helps ensure that all Vertex and Edge concrete classes
 * get properly serialized and deserialized by stripping them of their concrete class name so that they are treated
 * generically. See the {@link #getRegistration(Class)} method for the core of this logic.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGryoClassResolver implements ClassResolver {
    static public final byte NAME = -1;

    protected Kryo kryo;

    protected final IntMap<Registration> idToRegistration = new IntMap<>();
    protected final ObjectMap<Class, Registration> classToRegistration = new ObjectMap<>();

    protected IdentityObjectIntMap<Class> classToNameId;
    protected IntMap<Class> nameIdToClass;
    protected ObjectMap<String, Class> nameToClass;
    protected int nextNameId;

    private int memoizedClassId = -1;
    private Registration memoizedClassIdValue;
    private Class memoizedClass;
    private Registration memoizedClassValue;

    @Override
    public void setKryo(Kryo kryo) {
        this.kryo = kryo;
    }

    @Override
    public Registration register(final Registration registration) {
        if (null == registration) throw new IllegalArgumentException("Registration cannot be null.");
        if (registration.getId() != NAME) idToRegistration.put(registration.getId(), registration);

        classToRegistration.put(registration.getType(), registration);
        if (registration.getType().isPrimitive())
            classToRegistration.put(getWrapperClass(registration.getType()), registration);
        return registration;
    }

    @Override
    public Registration registerImplicit(final Class type) {
        return register(new Registration(type, kryo.getDefaultSerializer(type), NAME));
    }

    /**
     * Called from {@link #getRegistration(Class)} to determine the actual type.
     */
    public abstract Class coerceType(final Class clazz);

    @Override
    public Registration getRegistration(final Class clazz) {
        final Class type = coerceType(clazz);

        if (type == memoizedClass) return memoizedClassValue;
        final Registration registration = classToRegistration.get(type);
        if (registration != null) {
            memoizedClass = type;
            memoizedClassValue = registration;
        }

        return registration;
    }

    @Override
    public Registration getRegistration(final int classID) {
        return idToRegistration.get(classID);
    }

    @Override
    public Registration writeClass(final Output output, final Class type) {
        if (null == type) {
            output.writeVarInt(Kryo.NULL, true);
            return null;
        }

        final Registration registration = kryo.getRegistration(type);
        if (registration.getId() == NAME)
            writeName(output, type);
        else
            output.writeVarInt(registration.getId() + 2, true);

        return registration;
    }

    protected void writeName(final Output output, final Class type) {
        output.writeVarInt(NAME + 2, true);
        if (classToNameId != null) {
            final int nameId = classToNameId.get(type, -1);
            if (nameId != -1) {
                output.writeVarInt(nameId, true);
                return;
            }
        }
        // Only write the class name the first time encountered in object graph.
        final int nameId = nextNameId++;
        if (classToNameId == null) classToNameId = new IdentityObjectIntMap<>();
        classToNameId.put(type, nameId);
        output.writeVarInt(nameId, true);
        output.writeString(type.getName());
    }

    @Override
    public Registration readClass(final Input input) {
        final int classID = input.readVarInt(true);
        switch (classID) {
            case Kryo.NULL:
                return null;
            case NAME + 2: // Offset for NAME and NULL.
                return readName(input);
        }

        if (classID == memoizedClassId) return memoizedClassIdValue;
        final Registration registration = idToRegistration.get(classID - 2);
        if (registration == null) throw new KryoException("Encountered unregistered class ID: " + (classID - 2));
        memoizedClassId = classID;
        memoizedClassIdValue = registration;
        return registration;
    }

    protected Registration readName(final Input input) {
        final int nameId = input.readVarInt(true);
        if (nameIdToClass == null) nameIdToClass = new IntMap<>();
        Class type = nameIdToClass.get(nameId);
        if (type == null) {
            // Only read the class name the first time encountered in object graph.
            final String className = input.readString();
            type = getTypeByName(className);
            if (type == null) {
                try {
                    type = Class.forName(className, false, kryo.getClassLoader());
                } catch (ClassNotFoundException ex) {
                    throw new KryoException("Unable to find class: " + className, ex);
                }
                if (nameToClass == null) nameToClass = new ObjectMap<>();
                nameToClass.put(className, type);
            }
            nameIdToClass.put(nameId, type);
        }
        return kryo.getRegistration(type);
    }

    protected Class<?> getTypeByName(final String className) {
        return nameToClass != null ? nameToClass.get(className) : null;
    }

    @Override
    public void reset() {
        if (!kryo.isRegistrationRequired()) {
            if (classToNameId != null) classToNameId.clear();
            if (nameIdToClass != null) nameIdToClass.clear();
            nextNameId = 0;
        }
    }
}