package com.tinkerpop.gremlin.driver.ser;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import com.tinkerpop.gremlin.structure.io.kryo.KryoMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.javatuples.Pair;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoMessageSerializerV1d0 implements MessageSerializer {
    private KryoMapper kryoMapper;
    private ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            return kryoMapper.createMapper();
        }
    };

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final String MIME_TYPE = SerTokens.MIME_KRYO_V1D0;
    private static final String MIME_TYPE_STRINGD = SerTokens.MIME_KRYO_V1D0 + "-stringd";

    private static final String TOKEN_EXTENDED_VERSION = "extendedVersion";
    private static final String TOKEN_CUSTOM = "custom";
    private static final String TOKEN_SERIALIZE_RESULT_TO_STRING = "serializeResultToString";

    private boolean serializeToString;

    /**
     * Creates an instance with a standard {@link com.tinkerpop.gremlin.structure.io.kryo.KryoMapper} instance. Note that this instance
     * will be overriden by {@link #configure} is called.
     */
    public KryoMessageSerializerV1d0() {
        kryoMapper = KryoMapper.build(KryoMapper.Version.V_1_0_0).create();
    }

    /**
     * Creates an instance with a provided mapper configured {@link com.tinkerpop.gremlin.structure.io.kryo.KryoMapper} instance. Note that this instance
     * will be overriden by {@link #configure} is called.
     */
    public KryoMessageSerializerV1d0(final KryoMapper kryo) {
        this.kryoMapper = kryo;
    }

    @Override
    public void configure(final Map<String, Object> config) {
        final byte extendedVersion;
        try {
            extendedVersion = Byte.parseByte(config.getOrDefault(TOKEN_EXTENDED_VERSION, KryoMapper.DEFAULT_EXTENDED_VERSION).toString());
        } catch (Exception ex) {
            throw new IllegalStateException(String.format("Invalid configuration value of [%s] for [%s] setting on %s serialization configuration",
                    config.getOrDefault(TOKEN_EXTENDED_VERSION, ""), TOKEN_EXTENDED_VERSION, this.getClass().getName()), ex);
        }

        final KryoMapper.Builder builder = KryoMapper.build(KryoMapper.Version.V_1_0_0).extendedVersion(extendedVersion);

        final List<String> classNameList;
        try {
            classNameList = (List<String>) config.getOrDefault(TOKEN_CUSTOM, new ArrayList<String>());
        } catch (Exception ex) {
            throw new IllegalStateException(String.format("Invalid configuration value of [%s] for [%s] setting on %s serialization configuration",
                    config.getOrDefault(TOKEN_CUSTOM, ""), TOKEN_CUSTOM, this.getClass().getName()), ex);
        }

        if (!classNameList.isEmpty()) {
            final List<Pair<Class, Function<Kryo, Serializer>>> classList = classNameList.stream().map(serializerDefinition -> {
                String className;
                Optional<String> serializerName;
                if (serializerDefinition.contains(";")) {
                    final String[] split = serializerDefinition.split(";");
                    if (split.length != 2)
                        throw new IllegalStateException(String.format("Invalid format for serializer definition [%s] - expected <class>:<serializer-class>", serializerDefinition));

                    className = split[0];
                    serializerName = Optional.of(split[1]);
                } else {
                    serializerName = Optional.empty();
                    className = serializerDefinition;
                }

                try {
                    final Class clazz = Class.forName(className);
                    final Serializer serializer;
                    if (serializerName.isPresent()) {
                        final Class serializerClazz = Class.forName(serializerName.get());
                        serializer = (Serializer) serializerClazz.newInstance();
                    } else
                        serializer = null;

                    return Pair.<Class, Function<Kryo, Serializer>>with(clazz, kryo -> serializer);
                } catch (Exception ex) {
                    throw new IllegalStateException("Class could not be found", ex);
                }
            }).collect(Collectors.toList());

            classList.forEach(c -> builder.addCustom(c.getValue0(), c.getValue1()));
        }

        this.serializeToString = Boolean.parseBoolean(config.getOrDefault(TOKEN_SERIALIZE_RESULT_TO_STRING, "false").toString());

        this.kryoMapper = builder.create();
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{this.serializeToString ? MIME_TYPE_STRINGD : MIME_TYPE};
    }

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        try {
            final Kryo kryo = kryoThreadLocal.get();
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            try (final Input input = new Input(payload)) {
                final Map<String, Object> responseData = (Map<String, Object>) kryo.readClassAndObject(input);
                final Map<String, Object> status = (Map<String, Object>) responseData.get(SerTokens.TOKEN_STATUS);
                final Map<String, Object> result = (Map<String, Object>) responseData.get(SerTokens.TOKEN_RESULT);
                return ResponseMessage.build(UUID.fromString(responseData.get(SerTokens.TOKEN_REQUEST).toString()))
                        .code(ResponseStatusCode.getFromValue((Integer) status.get(SerTokens.TOKEN_CODE)))
                        .statusMessage(Optional.ofNullable((String) status.get(SerTokens.TOKEN_MESSAGE)).orElse(""))
                        .statusAttributes((Map<String, Object>) status.get(SerTokens.TOKEN_ATTRIBUTES))
                        .result(result.get(SerTokens.TOKEN_DATA))
                        .responseMetaData((Map<String, Object>) result.get(SerTokens.TOKEN_META))
                        .create();
            }
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be deserialized by {}.", msg, KryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final Map<String, Object> result = new HashMap<>();
            result.put(SerTokens.TOKEN_DATA, serializeToString ? serializeResultToString(responseMessage) : responseMessage.getResult().getData());
            result.put(SerTokens.TOKEN_META, responseMessage.getResult().getMeta());

            final Map<String, Object> status = new HashMap<>();
            status.put(SerTokens.TOKEN_MESSAGE, responseMessage.getStatus().getMessage());
            status.put(SerTokens.TOKEN_CODE, responseMessage.getStatus().getCode().getValue());
            status.put(SerTokens.TOKEN_ATTRIBUTES, responseMessage.getStatus().getAttributes());

            final Map<String, Object> message = new HashMap<>();
            message.put(SerTokens.TOKEN_STATUS, status);
            message.put(SerTokens.TOKEN_RESULT, result);
            message.put(SerTokens.TOKEN_REQUEST, responseMessage.getRequestId() != null ? responseMessage.getRequestId() : null);

            final Kryo kryo = kryoThreadLocal.get();
            try (final OutputStream baos = new ByteArrayOutputStream()) {
                final Output output = new Output(baos);
                kryo.writeClassAndObject(output, message);

                final long size = output.total();
                if (size > Integer.MAX_VALUE)
                    throw new SerializationException(String.format("Message size of %s exceeds allocatable space", size));

                encodedMessage = allocator.buffer((int) output.total());
                encodedMessage.writeBytes(output.toBytes());
            }

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn("Response [{}] could not be serialized by {}.", responseMessage.toString(), KryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final ByteBuf msg) throws SerializationException {
        try {
            final Kryo kryo = kryoThreadLocal.get();
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            try (final Input input = new Input(payload)) {
                final Map<String, Object> requestData = (Map<String, Object>) kryo.readClassAndObject(input);
                final RequestMessage.Builder builder = RequestMessage.build((String) requestData.get(SerTokens.TOKEN_OP))
                        .overrideRequestId((UUID) requestData.get(SerTokens.TOKEN_REQUEST))
                        .processor((String) requestData.get(SerTokens.TOKEN_PROCESSOR));
                final Map<String, Object> args = (Map<String, Object>) requestData.get(SerTokens.TOKEN_ARGS);
                args.forEach(builder::addArg);
                return builder.create();
            }
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be deserialized by {}.", msg, KryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final Kryo kryo = kryoThreadLocal.get();
            try (final OutputStream baos = new ByteArrayOutputStream()) {
                final Output output = new Output(baos);
                final String mimeType = serializeToString ? MIME_TYPE_STRINGD : MIME_TYPE;
                output.writeByte(mimeType.length());
                output.write(mimeType.getBytes(UTF8));

                final Map<String, Object> request = new HashMap<>();
                request.put(SerTokens.TOKEN_REQUEST, requestMessage.getRequestId());
                request.put(SerTokens.TOKEN_PROCESSOR, requestMessage.getProcessor());
                request.put(SerTokens.TOKEN_OP, requestMessage.getOp());
                request.put(SerTokens.TOKEN_ARGS, requestMessage.getArgs());

                kryo.writeClassAndObject(output, request);

                final long size = output.total();
                if (size > Integer.MAX_VALUE)
                    throw new SerializationException(String.format("Message size of %s exceeds allocatable space", size));

                encodedMessage = allocator.buffer((int) size);
                encodedMessage.writeBytes(output.toBytes());
            }

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn("Request [{}] could not be serialized by {}.", requestMessage.toString(), KryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    private Object serializeResultToString(final ResponseMessage msg) {
        if (msg.getResult() == null) return "null";
        if (msg.getResult().getData() == null) return "null";

        // the IteratorHandler should return a collection so keep it as such
        final Object o = msg.getResult().getData();
        if (o instanceof Collection) {
            return ((Collection) o).stream().map(d -> null == d ? "null" : d.toString()).collect(Collectors.toList());
        } else {
            return o.toString();
        }
    }
}
