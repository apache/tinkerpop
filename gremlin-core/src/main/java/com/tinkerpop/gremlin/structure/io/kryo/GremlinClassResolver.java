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
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import static com.esotericsoftware.kryo.util.Util.getWrapperClass;

/**
 * This custom implementation of the {@code ClassResolver} helps ensure that all Vertex and Edge concrete classes
 * get properly serialized and deserialized by stripping them of their concrete class name so that they are treated
 * generically.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinClassResolver implements ClassResolver {
    static public final byte NAME = -1;

    protected Kryo kryo;

    protected final IntMap<Registration> idToRegistration = new IntMap();
    protected final ObjectMap<Class, Registration> classToRegistration = new ObjectMap();

    protected IdentityObjectIntMap<Class> classToNameId;
    protected IntMap<Class> nameIdToClass;
    protected ObjectMap<String, Class> nameToClass;
    protected int nextNameId;

    private int memoizedClassId = -1;
    private Registration memoizedClassIdValue;
    private Class memoizedClass;
    private Registration memoizedClassValue;

    public void setKryo (Kryo kryo) {
        this.kryo = kryo;
    }

    public Registration register (Registration registration) {
        if (registration == null) throw new IllegalArgumentException("registration cannot be null.");
        if (registration.getId() != NAME) {
            idToRegistration.put(registration.getId(), registration);
        }
        classToRegistration.put(registration.getType(), registration);
        if (registration.getType().isPrimitive()) classToRegistration.put(getWrapperClass(registration.getType()), registration);
        return registration;
    }

    public Registration registerImplicit (Class type) {
        return register(new Registration(type, kryo.getDefaultSerializer(type), NAME));
    }

    public Registration getRegistration (Class clazz) {
        final Class type;
        if (Vertex.class.isAssignableFrom(clazz))
            type = Vertex.class;
        else if (Edge.class.isAssignableFrom(clazz))
            type = Edge.class;
        else
            type = clazz;

        if (type == memoizedClass) return memoizedClassValue;
        Registration registration = classToRegistration.get(type);
        if (registration != null) {
            memoizedClass = type;
            memoizedClassValue = registration;
        }
        return registration;
    }

    public Registration getRegistration (int classID) {
        return idToRegistration.get(classID);
    }

    public Registration writeClass (Output output, Class type) {
        if (type == null) {
            output.writeVarInt(Kryo.NULL, true);
            return null;
        }
        Registration registration = kryo.getRegistration(type);
        if (registration.getId() == NAME)
            writeName(output, type, registration);
        else {
            output.writeVarInt(registration.getId() + 2, true);
        }
        return registration;
    }

    protected void writeName (Output output, Class type, Registration registration) {
        output.writeVarInt(NAME + 2, true);
        if (classToNameId != null) {
            int nameId = classToNameId.get(type, -1);
            if (nameId != -1) {
                output.writeVarInt(nameId, true);
                return;
            }
        }
        // Only write the class name the first time encountered in object graph.
        int nameId = nextNameId++;
        if (classToNameId == null) classToNameId = new IdentityObjectIntMap();
        classToNameId.put(type, nameId);
        output.writeVarInt(nameId, true);
        output.writeString(type.getName());
    }

    public Registration readClass (Input input) {
        int classID = input.readVarInt(true);
        switch (classID) {
            case Kryo.NULL:
                return null;
            case NAME + 2: // Offset for NAME and NULL.
                return readName(input);
        }
        if (classID == memoizedClassId) return memoizedClassIdValue;
        Registration registration = idToRegistration.get(classID - 2);
        if (registration == null) throw new KryoException("Encountered unregistered class ID: " + (classID - 2));
        memoizedClassId = classID;
        memoizedClassIdValue = registration;
        return registration;
    }

    protected Registration readName (Input input) {
        int nameId = input.readVarInt(true);
        if (nameIdToClass == null) nameIdToClass = new IntMap();
        Class type = nameIdToClass.get(nameId);
        if (type == null) {
            // Only read the class name the first time encountered in object graph.
            String className = input.readString();
            type = getTypeByName(className);
            if (type == null) {
                try {
                    type = Class.forName(className, false, kryo.getClassLoader());
                } catch (ClassNotFoundException ex) {
                    throw new KryoException("Unable to find class: " + className, ex);
                }
                if (nameToClass == null) nameToClass = new ObjectMap();
                nameToClass.put(className, type);
            }
            nameIdToClass.put(nameId, type);
        }
        return kryo.getRegistration(type);
    }

    protected Class<?> getTypeByName(final String className) {
        return nameToClass != null ? nameToClass.get(className) : null;
    }

    public void reset () {
        if (!kryo.isRegistrationRequired()) {
            if (classToNameId != null) classToNameId.clear();
            if (nameIdToClass != null) nameIdToClass.clear();
            nextNameId = 0;
        }
    }
}