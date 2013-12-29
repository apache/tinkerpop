package com.tinkerpop.gremlin.server;

import org.json.JSONObject;

/**
 * Serializes a single result from the ScripEngine.  Typically this will be an item from an iterator.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ResultSerializer {
    public static final ToStringResultSerializer TO_STRING_RESULT_SERIALIZER = new ToStringResultSerializer();
    public static final JsonResultSerializer JSON_RESULT_SERIALIZER = new JsonResultSerializer();

    public default String serialize(final Object o, final Context context) {
        return this.serialize(o, ResultCode.SUCCESS, context);
    }

    public String serialize(final Object o, final ResultCode code, final Context context);

    /**
     * Choose a serializer based on the "accept" argument on the message, where "accept" is a mime type.
     */
    public static ResultSerializer select(final String accept) {
        if (accept.equals("application/json"))
            return JSON_RESULT_SERIALIZER;
        else
            return TO_STRING_RESULT_SERIALIZER;
    }

    /**
     * Use toString() to serialize the result.
     */
    public static class ToStringResultSerializer implements ResultSerializer {

        private static final String TEXT_RESPONSE_FORMAT_WITH_RESULT = "%s>>%s";
        private static final String TEXT_RESPONSE_FORMAT_WITH_NULL = "%s>>null";

        @Override
        public String serialize(final Object o, final ResultCode code, final Context context) {
            return o == null ?
                    String.format(TEXT_RESPONSE_FORMAT_WITH_NULL, context.getRequestMessage().requestId) :
                    String.format(TEXT_RESPONSE_FORMAT_WITH_RESULT, context.getRequestMessage().requestId, o.toString());
        }
    }

    /**
     * Converts a result to JSON.
     */
    public static class JsonResultSerializer implements ResultSerializer {
        public static final String TOKEN_RESULT = "result";
        public static final String TOKEN_ID = "id";
        public static final String TOKEN_TYPE = "type";
        public static final String TOKEN_KEY = "key";
        public static final String TOKEN_VALUE = "value";
        public static final String TOKEN_CODE = "code";
        public static final String TOKEN_PROPERTIES = "properties";
        public static final String TOKEN_META = "meta";
        public static final String TOKEN_EDGE = "edge";
        public static final String TOKEN_VERTEX = "vertex";
        public static final String TOKEN_REQUEST = "requestId";
        public static final String TOKEN_IN = "in";
        public static final String TOKEN_OUT = "out";
        public static final String TOKEN_LABEL = "label";

        @Override
        public String serialize(final Object o, final ResultCode code, final Context context) {
            try {
                final JSONObject result = new JSONObject();
                result.put(TOKEN_CODE, code.getValue());
                //result.put(TOKEN_RESULT, prepareOutput(o));

                // a context may not be available
                if (context != null)
                    result.put(TOKEN_REQUEST, context.getRequestMessage().requestId);

                return result.toString();
            } catch (Exception ex) {
                throw new RuntimeException("Error during serialization.", ex);
            }
        }

        /**
         * Serializes results.  The order in which the "object" is evaluated has importance to proper serialization.
         */
        /*private Object prepareOutput(final Object object) throws Exception {
            if (object == null)
                return JSONObject.NULL;
            else if (object instanceof Property) {
                final Property t = (Property) object;
                final JSONObject jsonObject = new JSONObject();
                jsonObject.put(TOKEN_VALUE, prepareOutput(t.orElse(null)));

                if (t.getProperties().size() > 0) {
                    final JSONObject metaProperties = new JSONObject();
                    t.getPropertyKeys().forEach(k -> {
                        try {
                            metaProperties.put(k.toString(), prepareOutput(t.getProperty(k.toString())));
                        } catch (Exception ex) {
                            // there can't be null keys on an element so don't think there is a need to launch
                            // a JSONException here.
                            throw new RuntimeException(ex);
                        }
                    });
                    jsonObject.put(TOKEN_META, metaProperties);
                }
                return jsonObject;
            } else if (object instanceof Edge.Property) {
                final Edge.Property t = (Edge.Property) object;
                final JSONObject jsonObject = new JSONObject();
                jsonObject.put(TOKEN_VALUE, prepareOutput(t.orElse(null)));
                return jsonObject;
            } else if (object instanceof com.tinkerpop.blueprints.Property) {
                // catches meta-properties on vertices
                return prepareOutput(((com.tinkerpop.blueprints.Property) object).getValue());
            } else if (object instanceof Element) {
                final Element element = (Element) object;
                final JSONObject jsonObject = new JSONObject();
                jsonObject.put(TOKEN_ID, prepareOutput(element.getId()));
                jsonObject.put(TOKEN_TYPE, element instanceof Edge ? TOKEN_EDGE : TOKEN_VERTEX);

                if (object instanceof Edge) {
                    final Edge e = (Edge) object;
                    jsonObject.put(TOKEN_IN, prepareOutput(e.getVertex(Direction.IN).getId()));
                    jsonObject.put(TOKEN_OUT, prepareOutput(e.getVertex(Direction.OUT).getId()));
                    jsonObject.put(TOKEN_LABEL, e.getLabel());
                }

                final JSONObject jsonProperties = new JSONObject();
                element.getPropertyKeys().forEach(k -> {
                    try {
                        jsonProperties.put(k, prepareOutput(element.getProperty(k)));
                    } catch (Exception ex) {
                        // there can't be null keys on an element so don't think there is a need to launch
                        // a JSONException here.
                        throw new RuntimeException(ex);
                    }
                });
                jsonObject.put(TOKEN_PROPERTIES, jsonProperties);
                return jsonObject;
                //} else if (object instanceof Row) {} todo: get Table/Row in when implemented
            } else if (object instanceof Map) {
                final JSONObject jsonObject = new JSONObject();
                final Map map = (Map) object;
                for (Object key : map.keySet()) {
                    // force an error here by passing in a null key to the JSONObject.  That way a good error message
                    // gets back to the user.
                    if (key instanceof Element) {
                        final Element element = (Element) key;
                        final HashMap<String, Object> m = new HashMap<>();
                        m.put(TOKEN_KEY, this.prepareOutput(element));
                        m.put(TOKEN_VALUE, this.prepareOutput(map.get(key)));

                        jsonObject.put(element.getId().toString(), new JSONObject(m));
                    } else {
                        jsonObject.put(key == null ? null : key.toString(), this.prepareOutput(map.get(key)));
                    }
                }
                return jsonObject;
            } else if (object instanceof Iterable || object instanceof Iterator) {
                final JSONArray jsonArray = new JSONArray();
                final Iterator itty = object instanceof Iterator ? (Iterator) object : ((Iterable) object).iterator();
                while (itty.hasNext()) {
                    jsonArray.put(prepareOutput(itty.next()));
                }
                return jsonArray;
            } else if (object instanceof Number || object instanceof Boolean)
                return object;
            else if (object == JSONObject.NULL)
                return JSONObject.NULL;
            else
                return object.toString();
        }
    } */
    }
}
