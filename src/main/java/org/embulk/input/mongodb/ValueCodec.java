package org.embulk.input.mongodb;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.msgpack.value.ValueFactory.*;

public class ValueCodec implements Codec<Value>
{
    private final SimpleDateFormat formatter;
    private final Logger log = Exec.getLogger(MongodbInputPlugin.class);
    private final boolean stopOnInvalidRecord;
    private final MongodbInputPlugin.PluginTask task;

    public ValueCodec(boolean stopOnInvalidRecord, MongodbInputPlugin.PluginTask task)
    {
        this.formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", java.util.Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.stopOnInvalidRecord = stopOnInvalidRecord;
        this.task = task;
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
            String fieldName = reader.readName();
            BsonType type = reader.getCurrentBsonType();
            fieldName = normalize(fieldName);

            try {
                kvs.put(newString(fieldName), readValue(reader, decoderContext));
            }
            catch (UnknownTypeFoundException ex) {
                reader.skipValue();
                if (stopOnInvalidRecord) {
                    throw ex;
                }
                log.warn(String.format("Skipped document because field '%s' contains unsupported object type [%s]",
                        fieldName, type));
            }
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
            default: // e.g. MIN_KEY, MAX_KEY, SYMBOL, DB_POINTER, UNDEFINED
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

    public static class UnknownTypeFoundException extends DataException
    {
        UnknownTypeFoundException(String message)
        {
            super(message);
        }
    }
}
