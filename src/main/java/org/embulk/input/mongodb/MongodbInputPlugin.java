package org.embulk.input.mongodb;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import javax.validation.constraints.Min;
import java.net.UnknownHostException;
import java.util.List;

public class MongodbInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        // MongoDB connection string URI
        @Config("uri")
        String getUri();

        @Config("collection")
        String getCollection();

        @Config("fields")
        @ConfigDefault("null")
        Optional<SchemaConfig> getFields();

        @Config("projection")
        @ConfigDefault("\"{}\"")
        String getProjection();

        @Config("query")
        @ConfigDefault("\"{}\"")
        String getQuery();

        @Config("sort")
        @ConfigDefault("\"{}\"")
        String getSort();

        @Config("batch_size")
        @ConfigDefault("10000")
        @Min(1)
        int getBatchSize();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();

        @Config("json_column_name")
        @ConfigDefault("\"record\"")
        String getJsonColumnName();

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    private final Logger log = Exec.getLogger(MongodbInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        if (task.getFields().isPresent()) {
            throw new ConfigException("field option was deprecated so setting will be ignored");
        }

        validateJsonField("projection", task.getProjection());
        validateJsonField("query", task.getQuery());
        validateJsonField("sort", task.getSort());

        // Connect once to throw ConfigException in earlier stage of excecution
        try {
            connect(task);
        } catch (UnknownHostException | MongoException ex) {
            throw new ConfigException(ex);
        }
        Schema schema = Schema.builder().add(task.getJsonColumnName(), Types.JSON).build();
        return resume(task.dump(), schema, 1, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successCommitReports)
    {
        // do nothing
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);
        final Column column = pageBuilder.getSchema().getColumns().get(0);

        MongoCollection<Value> collection;
        try {
            MongoDatabase db = connect(task);

            CodecRegistry registry = CodecRegistries.fromRegistries(
                    MongoClient.getDefaultCodecRegistry(),
                    CodecRegistries.fromCodecs(new ValueCodec(task.getStopOnInvalidRecord()))
            );
            collection = db.getCollection(task.getCollection(), Value.class)
                    .withCodecRegistry(registry);
        } catch (UnknownHostException | MongoException ex) {
            throw new ConfigException(ex);
        }

        Bson query = (Bson) JSON.parse(task.getQuery());
        Bson projection = (Bson) JSON.parse(task.getProjection());
        Bson sort = (Bson) JSON.parse(task.getSort());

        log.trace("query: {}", query);
        log.trace("projection: {}", projection);
        log.trace("sort: {}", sort);

        try (MongoCursor<Value> cursor = collection
                .find(query)
                .projection(projection)
                .sort(sort)
                .batchSize(task.getBatchSize())
                .iterator()) {
            while (cursor.hasNext()) {
                pageBuilder.setJson(column, cursor.next());
                pageBuilder.addRecord();
            }
        } catch (MongoException ex) {
            Throwables.propagate(ex);
        }

        pageBuilder.finish();

        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private MongoDatabase connect(final PluginTask task) throws UnknownHostException, MongoException {
        MongoClientURI uri = new MongoClientURI(task.getUri());
        MongoClient mongoClient = new MongoClient(uri);

        MongoDatabase db = mongoClient.getDatabase(uri.getDatabase());
        // Get collection count for throw Exception
        db.getCollection(task.getCollection()).count();
        return db;
    }

    private void validateJsonField(String name, String jsonString) {
        try {
            JSON.parse(jsonString);
        } catch (JSONParseException ex) {
            throw new ConfigException(String.format("Invalid JSON string was given for '%s' parameter. [%s]", name, jsonString));
        }
    }
}
