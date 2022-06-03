/*
 * Copyright 2015 Kazuyuki Honda, and the Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.input.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
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
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MongodbInputPlugin
        implements InputPlugin
{
    private final Logger log = LoggerFactory.getLogger(MongodbInputPlugin.class);

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);

        if (task.getFields().isPresent()) {
            throw new ConfigException("field option was deprecated so setting will be ignored");
        }
        if (task.getIncrementalField().isPresent() && !task.getSort().equals("{}")) {
            throw new ConfigException("both of sort and incremental_load can't be used together");
        }
        if (task.getIncrementalField().isPresent() && task.getSkip().isPresent()) {
            throw new ConfigException("both of skip and incremental_load can't be used together");
        }

        if (task.getAggregation().isPresent()) {
            if (task.getIncrementalField().isPresent()) {
                throw new ConfigException("both of aggregation and incremental_load can't be used together");
            }
            if (!task.getSort().equals("{}")) {
                throw new ConfigException("both of sort and aggregation can't be used together");
            }
            if (task.getLimit().isPresent()) {
                throw new ConfigException("both of limit and aggregation can't be used together");
            }
            if (task.getSkip().isPresent()) {
                throw new ConfigException("both of skip and aggregation can't be used together");
            }
            if (!task.getQuery().equals("{}")) {
                throw new ConfigException("both of query and aggregation can't be used together");
            }
        }

        Map<String, String> newCondition = buildIncrementalCondition(task);
        task.setQuery(newCondition.get("query"));
        task.setSort(newCondition.get("sort"));

        validateJsonField("projection", task.getProjection());
        validateJsonField("query", task.getQuery());
        validateJsonField("sort", task.getSort());
        if (task.getAggregation().isPresent()) {
            validateJsonField("aggrigation", task.getAggregation().get());
        }

        // Connect once to throw ConfigException in earlier stage of excecution
        try {
            connect(task);
        }
        catch (UnknownHostException | MongoException ex) {
            throw new ConfigException(ex);
        }
        Schema schema = Schema.builder().add(task.getJsonColumnName(), Types.JSON).build();
        return resume(task.toTaskSource(), schema, 1, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        List<TaskReport> report = control.run(taskSource, schema, taskCount);

        ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();
        if (report.size() > 0 && report.get(0).has("last_record")) {
            configDiff.set("last_record", report.get(0).get(Map.class, "last_record"));
        }

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
        // do nothing
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        BufferAllocator allocator = Exec.getBufferAllocator();
        PageBuilder pageBuilder = Exec.getPageBuilder(allocator, schema, output);
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

        if (task.getAggregation().isPresent()) {
            Bson aggregationString = Document.parse(task.getAggregation().get());
            List<Bson> aggregation = Arrays.asList(aggregationString);
            try (MongoCursor<Value> cursor = collection
                    .aggregate(aggregation).iterator()) {
                while (cursor.hasNext()) {
                    pageBuilder.setJson(column, cursor.next());
                    pageBuilder.addRecord();
                }
            } catch (MongoException ex) {
                if (ex instanceof RuntimeException) {
                    throw ex;
                }
                throw new RuntimeException(ex);
            }
        }
        else {
            try (MongoCursor<Value> cursor = collection
                    .find(query)
                    .projection(projection)
                    .sort(sort)
                    .batchSize(task.getBatchSize())
                    .limit(task.getLimit().orElse(0))
                    .skip(task.getSkip().orElse(0))
                    .iterator()) {
                while (cursor.hasNext()) {
                    pageBuilder.setJson(column, cursor.next());
                    pageBuilder.addRecord();
                }
            } catch (MongoException ex) {
                if (ex instanceof RuntimeException) {
                    throw ex;
                }
                throw new RuntimeException(ex);
            }
        }

        pageBuilder.finish();
        return updateTaskReport(CONFIG_MAPPER_FACTORY.newTaskReport(), valueCodec, task);
    }

    private TaskReport updateTaskReport(TaskReport report, ValueCodec valueCodec, PluginTask task)
    {
        final TaskReport lastRecord = CONFIG_MAPPER_FACTORY.newTaskReport();
        if (valueCodec.getLastRecord() != null && valueCodec.getProcessedRecordCount() > 0) {
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
        }
        else if (task.getIncrementalField().isPresent() && task.getLastRecord().isPresent()) {
            for (String field : task.getIncrementalField().get()) {
                if (task.getLastRecord().get().containsKey(field)) {
                    lastRecord.set(field, task.getLastRecord().get().get(field));
                }
            }
        }
        report.setNested("last_record", lastRecord);
        return report;
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
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
            return new MongoClient(addresses, Arrays.asList(createCredential(task)), createMongoClientOptions(task));
        }
        else {
            return new MongoClient(addresses, createMongoClientOptions(task));
        }
    }

    private MongoClientOptions createMongoClientOptions(PluginTask task)
    {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        if (task.getTls()) {
            builder.sslEnabled(true);
            if (task.getTlsInsecure()) {
                builder.sslInvalidHostNameAllowed(true);
                builder.sslContext(createSSLContextToAcceptAnyCert());
            }
        }
        return builder.build();
    }

    private SSLContext createSSLContextToAcceptAnyCert()
    {
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager()
                {
                    public X509Certificate[] getAcceptedIssuers()
                    {
                        return new X509Certificate[0];
                    }
                    public void checkClientTrusted(
                            X509Certificate[] certs, String authType)
                    {
                    }
                    public void checkServerTrusted(
                            X509Certificate[] certs, String authType)
                    {
                    }
                }
        };
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            return sc;
        }
        catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new ConfigException(e);
        }
    }

    // @see http://mongodb.github.io/mongo-java-driver/3.0/driver-async/reference/connecting/authenticating/
    private MongoCredential createCredential(PluginTask task)
    {
        MongoCredential credential;
        String authSource = task.getAuthSource().isPresent() ? task.getAuthSource().get() : task.getDatabase().get();
        AuthMethod authMethod = task.getAuthMethod().isPresent() ? task.getAuthMethod().get() : AuthMethod.AUTO;
        switch (authMethod) {
            case SCRAM_SHA_1:
                credential = MongoCredential.createScramSha1Credential(
                        task.getUser().get(),
                        authSource,
                        task.getPassword().get().toCharArray());
                break;
            case MONGODB_CR:
                credential = MongoCredential.createMongoCRCredential(
                        task.getUser().get(),
                        authSource,
                        task.getPassword().get().toCharArray());
                break;
            case AUTO:
            default:
                /* The client will negotiate the best mechanism based on the
                 * version of the server that the client is authenticating to.
                 * If the server version is 3.0 or higher, the driver will authenticate using the SCRAM-SHA-1 mechanism.
                 * Otherwise, the driver will authenticate using the MONGODB_CR mechanism.
                 */
                credential = MongoCredential.createCredential(
                        task.getUser().get(),
                        authSource,
                        task.getPassword().get().toCharArray()
                );
        }
        return credential;
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
            Document.parse(jsonString);
        }
        catch (JsonParseException ex) {
            throw new ConfigException(String.format("Invalid JSON string was given for '%s' parameter. [%s]", name, jsonString));
        }
    }
}
