package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.IdentityObjectIntMap;
import com.esotericsoftware.kryo.util.IntMap;
import com.esotericsoftware.kryo.util.ObjectMap;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import com.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

import static com.esotericsoftware.kryo.util.Util.getWrapperClass;

/**
 * This mapper implementation of the {@code ClassResolver} helps ensure that all Vertex and Edge concrete classes
 * get properly serialized and deserialized by stripping them of their concrete class name so that they are treated
 * generically.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinClassResolver implements ClassResolver {
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

    @Override
    public Registration getRegistration(final Class clazz) {
        // force all instances of Vertex, Edge, VertexProperty, etc. to their respective interface
        final Class type;
        if (!DetachedVertex.class.isAssignableFrom(clazz) && Vertex.class.isAssignableFrom(clazz))
            type = Vertex.class;
        else if (!DetachedEdge.class.isAssignableFrom(clazz) && Edge.class.isAssignableFrom(clazz))
            type = Edge.class;
        else if (!DetachedVertexProperty.class.isAssignableFrom(clazz) && VertexProperty.class.isAssignableFrom(clazz))
            type = VertexProperty.class;
        else if (!DetachedProperty.class.isAssignableFrom(clazz) && !DetachedVertexProperty.class.isAssignableFrom(clazz) && Property.class.isAssignableFrom(clazz))
            type = Property.class;
        else if (!DetachedPath.class.isAssignableFrom(clazz) && Path.class.isAssignableFrom(clazz))
            type = Path.class;
        else
            type = clazz;

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