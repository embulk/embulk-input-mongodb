package org.embulk.input.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.DataSource;
import org.embulk.config.DataSourceImpl;
import org.embulk.config.ModelManager;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MongodbInputPlugin
        implements InputPlugin
{
    private final Logger log = Exec.getLogger(MongodbInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        if (task.getFields().isPresent()) {
            throw new ConfigException("field option was deprecated so setting will be ignored");
        }
        if (task.getIncrementalField().isPresent() && !task.getSort().equals("{}")) {
            throw new ConfigException("both of sort and incremental_load can't be used together");
        }
        if (task.getIncrementalField().isPresent() && task.getSkip().isPresent()) {
            throw new ConfigException("both of skip and incremental_load can't be used together");
        }

        Map<String, String> newCondition = buildIncrementalCondition(task);
        task.setQuery(newCondition.get("query"));
        task.setSort(newCondition.get("sort"));

        validateJsonField("projection", task.getProjection());
        validateJsonField("query", task.getQuery());
        validateJsonField("sort", task.getSort());

        // Connect once to throw ConfigException in earlier stage of excecution
        try {
            connect(task);
        }
        catch (UnknownHostException | MongoException ex) {
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
        List<TaskReport> report = control.run(taskSource, schema, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();
        if (report.size() > 0 && report.get(0).has("last_record")) {
            configDiff.set("last_record", report.get(0).get(Map.class, "last_record"));
        }

        return configDiff;
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

        ValueCodec valueCodec = new ValueCodec(task.getStopOnInvalidRecord(), task);
        MongoCollection<Value> collection;
        try {
            MongoDatabase db = connect(task);

            CodecRegistry registry = CodecRegistries.fromRegistries(
                    MongoClient.getDefaultCodecRegistry(),
                    CodecRegistries.fromCodecs(valueCodec)
            );
            collection = db.getCollection(task.getCollection(), Value.class)
                    .withCodecRegistry(registry);
        }
        catch (UnknownHostException | MongoException ex) {
            throw new ConfigException(ex);
        }

        Bson query = BasicDBObject.parse(task.getQuery());
        Bson projection = BasicDBObject.parse(task.getProjection());
        Bson sort = BasicDBObject.parse(task.getSort());

        log.trace("query: {}", query);
        log.trace("projection: {}", projection);
        log.trace("sort: {}", sort);
        if (task.getLimit().isPresent()) {
            log.trace("limit: {}", task.getLimit());
        }
        if (task.getSkip().isPresent()) {
            log.trace("skip: {}", task.getSkip());
        }

        try (MongoCursor<Value> cursor = collection
                .find(query)
                .projection(projection)
                .sort(sort)
                .batchSize(task.getBatchSize())
                .limit(task.getLimit().or(0))
                .skip(task.getSkip().or(0))
                .iterator()) {
            while (cursor.hasNext()) {
                pageBuilder.setJson(column, cursor.next());
                pageBuilder.addRecord();
            }
        } catch (MongoException ex) {
            Throwables.propagate(ex);
        }

        pageBuilder.finish();

        TaskReport report = Exec.newTaskReport();

        if (valueCodec.getLastRecord() != null) {
            DataSource lastRecord = new DataSourceImpl(Exec.getInjector().getInstance(ModelManager.class));
            for (String k : valueCodec.getLastRecord().keySet()) {
                String value = valueCodec.getLastRecord().get(k).toString();
                Map<String, String> types = valueCodec.getLastRecordType();
                HashMap<String, String> innerValue = new HashMap<>();
                switch(types.get(k)) {
                    case "OBJECT_ID":
                        innerValue.put("$oid", value);
                        lastRecord.set(k, innerValue);
                        break;
                    case "DATE_TIME":
                        innerValue.put("$date", value);
                        lastRecord.set(k, innerValue);
                        break;
                    case "INT32":
                    case "INT64":
                    case "TIMESTAMP":
                        lastRecord.set(k, Integer.valueOf(value));
                        break;
                    case "BOOLEAN":
                        lastRecord.set(k, Boolean.valueOf(value));
                        break;
                    case "DOUBLE":
                        lastRecord.set(k, Double.valueOf(value));
                        break;
                    case "DOCUMENT":
                    case "ARRAY":
                        throw new ConfigException(String.format("Unsupported type '%s' was given for 'last_record' [%s]", types.get(k), value));
                    default:
                        lastRecord.set(k, value);
                }
            }
            report.setNested("last_record", lastRecord);
        }
        return report;
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private MongoDatabase connect(final PluginTask task) throws UnknownHostException, MongoException
    {
        MongoClient mongoClient;
        String database;

        if (!task.getUri().isPresent() && !task.getHosts().isPresent()) {
            throw new ConfigException("'uri' or 'hosts' is required");
        }

        if (task.getUri().isPresent()) {
            MongoClientURI uri = new MongoClientURI(task.getUri().get());
            database = uri.getDatabase();
            mongoClient = new MongoClient(uri);
        }
        else {
            mongoClient = createClientFromParams(task);
            database = task.getDatabase().get();
        }

        MongoDatabase db = mongoClient.getDatabase(database);
        // Get collection count for throw Exception
        db.getCollection(task.getCollection()).count();
        return db;
    }

    private MongoClient createClientFromParams(PluginTask task)
    {
        if (!task.getHosts().isPresent()) {
            throw new ConfigException("'hosts' option's value is required but empty");
        }
        if (!task.getDatabase().isPresent()) {
            throw new ConfigException("'database' option's value is required but empty");
        }

        List<ServerAddress> addresses = new ArrayList<>();
        for (HostTask host : task.getHosts().get()) {
            addresses.add(new ServerAddress(host.getHost(), host.getPort()));
        }

        if (task.getUser().isPresent()) {
            MongoCredential credential = MongoCredential.createCredential(
                    task.getUser().get(),
                    task.getDatabase().get(),
                    task.getPassword().get().toCharArray()
            );
            return new MongoClient(addresses, Arrays.asList(credential));
        }
        else {
            return new MongoClient(addresses);
        }
    }

    private Map<String, String> buildIncrementalCondition(PluginTask task)
    {
        Map<String, String> result = new HashMap<>();
        String query = task.getQuery();
        String sort = task.getSort();
        result.put("query", query);
        result.put("sort", sort);

        Optional<List<String>> incrementalField = task.getIncrementalField();
        Optional<Map<String, Object>> lastRecord = task.getLastRecord();
        if (!incrementalField.isPresent()) {
            return result;
        }

        Map<String, Object> newQuery = new LinkedHashMap<>();
        Map<String, Integer> newSort = new LinkedHashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> queryJson = mapper.readValue(query, Map.class);
            for (String k : queryJson.keySet()) {
                newQuery.put(k, queryJson.get(k));
            }

            if (lastRecord.isPresent()) {
                for (String k : lastRecord.get().keySet()) {
                    Map<String, Object> v = new HashMap<>();
                    Object record = lastRecord.get().get(k);

                    if (newQuery.containsKey(k)) {
                        throw new ConfigException("Field declaration was duplicated between 'incremental_field' and 'query' options");
                    }

                    v.put("$gt", record);
                    newQuery.put(k, v);
                }
                String newQueryString = mapper.writeValueAsString(newQuery);
                log.info(String.format("New query value was generated for incremental load: '%s'", newQueryString));
                result.put("query", newQueryString);
            }

            for (String k : incrementalField.get()) {
                newSort.put(k, 1);
            }

            String newSortString = mapper.writeValueAsString(newSort);
            log.info(String.format("New sort value was generated for incremental load: '%s'", newSortString));
            result.put("sort", newSortString);

            return result;
        }
        catch (JsonParseException | IOException ex) {
            throw new ConfigException("Could not generate new query for incremental load.");
        }
    }

    private void validateJsonField(String name, String jsonString)
    {
        try {
            BasicDBObject.parse(jsonString);
        }
        catch (JsonParseException ex) {
            throw new ConfigException(String.format("Invalid JSON string was given for '%s' parameter. [%s]", name, jsonString));
        }
    }
}
