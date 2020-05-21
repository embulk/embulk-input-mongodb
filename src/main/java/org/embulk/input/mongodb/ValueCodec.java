package org.embulk.input.mongodb;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.embulk.spi.DataException;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import static org.msgpack.value.ValueFactory.newArray;
import static org.msgpack.value.ValueFactory.newBinary;
import static org.msgpack.value.ValueFactory.newBoolean;
import static org.msgpack.value.ValueFactory.newFloat;
import static org.msgpack.value.ValueFactory.newInteger;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newNil;
import static org.msgpack.value.ValueFactory.newString;

public class ValueCodec implements Codec<Value>
{
    private final SimpleDateFormat formatter;
    private final Logger log = LoggerFactory.getLogger(MongodbInputPlugin.class);
    private final boolean stopOnInvalidRecord;
    private final PluginTask task;
    private final Optional<List<String>> incrementalField;
    private Map<String, Object> lastRecord;
    private long processedRecordCount = 0;
    private Map<String, String> lastRecordType;

    public ValueCodec(boolean stopOnInvalidRecord, PluginTask task)
    {
        this.formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", java.util.Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.stopOnInvalidRecord = stopOnInvalidRecord;
        this.task = task;
        this.incrementalField = task.getIncrementalField();
        this.lastRecord = new HashMap<>();
        this.lastRecordType = new HashMap<>();
    }

    @Override
    public void encode(final BsonWriter writer, final Value value, final EncoderContext encoderContext)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value decode(final BsonReader reader, final DecoderContext decoderContext)
    {
        Map<Value, Value> kvs = new LinkedHashMap<>();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String originalFieldName = reader.readName();
            BsonType type = reader.getCurrentBsonType();
            String fieldName = normalize(originalFieldName);

            Value value;
            try {
                value = readValue(reader, decoderContext);
                kvs.put(newString(fieldName), value);
                if (incrementalField.isPresent() && incrementalField.get().contains(originalFieldName)) {
                    this.lastRecord.put(originalFieldName, value);
                    this.lastRecordType.put(originalFieldName, type.toString());
                }
            }
            catch (UnknownTypeFoundException ex) {
                reader.skipValue();
                if (stopOnInvalidRecord) {
                    throw ex;
                }
                log.warn(String.format("Skipped document because field '%s' contains unsupported object type [%s]",
                        fieldName, type));
            }
            this.processedRecordCount++;
        }
        reader.readEndDocument();

        return newMap(kvs);
    }

    public Value decodeArray(final BsonReader reader, final DecoderContext decoderContext)
    {
        List<Value> list = new ArrayList<>();

        reader.readStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext));
        }
        reader.readEndArray();

        return newArray(list);
    }

    private Value readValue(BsonReader reader, DecoderContext decoderContext)
    {
        switch (reader.getCurrentBsonType()) {
            // https://docs.mongodb.com/manual/reference/bson-types/
            // https://github.com/mongodb/mongo-java-driver/tree/master/bson/src/main/org/bson/codecs
            case DOUBLE:
                return newFloat(reader.readDouble());
            case STRING:
                return newString(reader.readString());
            case ARRAY:
                return decodeArray(reader, decoderContext);
            case BINARY:
                return newBinary(reader.readBinaryData().getData(), true);
            case OBJECT_ID:
                return newString(reader.readObjectId().toString());
            case BOOLEAN:
                return newBoolean(reader.readBoolean());
            case DATE_TIME:
                return newString(formatter.format(new Date(reader.readDateTime())));
            case NULL:
                reader.readNull();
                return newNil();
            case REGULAR_EXPRESSION:
                return newString(reader.readRegularExpression().toString());
            case JAVASCRIPT:
                return newString(reader.readJavaScript());
            case JAVASCRIPT_WITH_SCOPE:
                return newString(reader.readJavaScriptWithScope());
            case INT32:
                return newInteger(reader.readInt32());
            case TIMESTAMP:
                return newInteger(reader.readTimestamp().getTime());
            case INT64:
                return newInteger(reader.readInt64());
            case DOCUMENT:
                return decode(reader, decoderContext);
            case SYMBOL:
                return newString(reader.readSymbol());
            default: // e.g. MIN_KEY, MAX_KEY, DB_POINTER, UNDEFINED
                throw new UnknownTypeFoundException(String.format("Unsupported type %s of '%s' field. Please exclude the field from 'projection:' option",
                        reader.getCurrentBsonType(), reader.getCurrentName()));
        }
    }

    @Override
    public Class<Value> getEncoderClass()
    {
        return Value.class;
    }

    private String normalize(String key)
    {
        if (key.equals("_id")) {
            return task.getIdFieldName();
        }
        return key;
    }

    public Map<String, Object> getLastRecord()
    {
        return this.lastRecord;
    }

    public Long getProcessedRecordCount()
    {
        return this.processedRecordCount;
    }

    public Map<String, String> getLastRecordType()
    {
        return this.lastRecordType;
    }

    public static class UnknownTypeFoundException extends DataException
    {
        UnknownTypeFoundException(String message)
        {
            super(message);
        }
    }
}
