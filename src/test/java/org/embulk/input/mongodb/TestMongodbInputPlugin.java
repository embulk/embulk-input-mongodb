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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonBinary;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonMaxKey;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Symbol;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Types;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestMongodbInputPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    private final String mongoUri = "mongodb://mongo_user:dbpass@localhost:27017/mydb";
    private final String mongoCollection = "my_collection";

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ConfigSource config;
    private MongodbInputPlugin plugin;
    private MockPageOutput output;

    @Before
    public void createResources()
    {
        config = config();
        plugin = new MongodbInputPlugin();
        output = new MockPageOutput();
    }

    @Test
    public void checkDefaultValues()
    {
        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection);

        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        assertEquals("{}", task.getQuery());
        assertEquals("{}", task.getSort());
        assertEquals(Optional.<Integer>empty(), task.getLimit());
        assertEquals(Optional.<Integer>empty(), task.getSkip());
        assertEquals((long) 10000, (long) task.getBatchSize());
        assertEquals("record", task.getJsonColumnName());
        assertEquals(Optional.empty(), task.getIncrementalField());
        assertEquals(Optional.empty(), task.getLastRecord());
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesUriIsNull()
    {
        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", null)
                .set("collection", mongoCollection);

        plugin.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesInvalidUri()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", "mongodb://mongouser:password@non-exists.example.com:23490/test")
                .set("collection", mongoCollection);

        plugin.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesCollectionIsNull()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", null);

        plugin.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkSortCannotUseWithIncremental()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("sort", "{ \"field1\": 1 }")
                .set("incremental_field", Optional.of(Arrays.asList("account")));

        plugin.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkSkipCannotUseWithIncremental()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("skip", 10)
                .set("incremental_field", Optional.of(Arrays.asList("account")));

        plugin.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkInvalidQueryOption()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("query", "{\"key\":invalid_value}")
                .set("last_record", 0)
                .set("incremental_field", Optional.of(Arrays.asList("account")));

        plugin.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkAggregationWithOtherOption()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("query", "{\"key\":\"valid_value\"}")
                .set("aggregation", "{$match: { account: { $gt: 32864}}}")
                .set("incremental_field", Optional.of(Arrays.asList("account")));

        plugin.transaction(config, new Control());
    }

    @Test
    public void testCreateCredentialsSha1() throws Exception
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(
                configForAuth().deepCopy()
                        .set("auth_method", "scram-sha-1")
                        .set("database", "db"),
                PluginTask.class);

        Method createCredential = MongodbInputPlugin.class.getDeclaredMethod("createCredential", PluginTask.class);
        createCredential.setAccessible(true);
        MongoCredential credential = (MongoCredential) createCredential.invoke(plugin, task);
        assertThat("SCRAM-SHA-1", is(credential.getMechanism()));
        assertThat("db", is(credential.getSource()));
    }

    @Test
    public void testCreateCredentialsSha1WithAuthSource() throws Exception
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(
                configForAuth().deepCopy()
                        .set("auth_method", "scram-sha-1")
                        .set("database", "db")
                        .set("auth_source", "authdb"),
                PluginTask.class);

        Method createCredential = MongodbInputPlugin.class.getDeclaredMethod("createCredential", PluginTask.class);
        createCredential.setAccessible(true);
        MongoCredential credential = (MongoCredential) createCredential.invoke(plugin, task);
        assertThat("SCRAM-SHA-1", is(credential.getMechanism()));
        assertThat("authdb", is(credential.getSource()));
    }

    @Test
    public void testCreateCredentialsCr() throws Exception
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(
                configForAuth().deepCopy()
                        .set("auth_method", "mongodb-cr"),
                PluginTask.class);

        Method createCredential = MongodbInputPlugin.class.getDeclaredMethod("createCredential", PluginTask.class);
        createCredential.setAccessible(true);
        MongoCredential credential = (MongoCredential) createCredential.invoke(plugin, task);
        assertThat("MONGODB-CR", is(credential.getMechanism()));
    }

    @Test
    public void testCreateMongoClientOptionsTLSEnableWithInsecureEnable() throws Exception
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(
                configForAuth().deepCopy()
                        .set("tls", "true")
                        .set("tls_insecure", "true"),
                PluginTask.class);
        Method createMongoClientOptions = MongodbInputPlugin.class.getDeclaredMethod("createMongoClientOptions", PluginTask.class);
        createMongoClientOptions.setAccessible(true);
        MongoClientOptions mongoClientOptions = (MongoClientOptions) createMongoClientOptions.invoke(plugin, task);
        assertThat(true, is(mongoClientOptions.isSslEnabled()));
        assertThat(true, is(mongoClientOptions.isSslInvalidHostNameAllowed()));
    }

    @Test
    public void testCreateMongoClientOptionsTLSEnableWithInsecureDisable() throws Exception
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(
                configForAuth().deepCopy()
                        .set("tls", "true"),
                PluginTask.class);
        Method createMongoClientOptions = MongodbInputPlugin.class.getDeclaredMethod("createMongoClientOptions", PluginTask.class);
        createMongoClientOptions.setAccessible(true);
        MongoClientOptions mongoClientOptions = (MongoClientOptions) createMongoClientOptions.invoke(plugin, task);
        assertThat(true, is(mongoClientOptions.isSslEnabled()));
        assertThat(false, is(mongoClientOptions.isSslInvalidHostNameAllowed()));
    }

    @Test
    public void testCreateMongoClientOptionsTLSDisable() throws Exception
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(
                configForAuth().deepCopy()
                        .set("tls", "false"),
                PluginTask.class);
        Method createMongoClientOptions = MongodbInputPlugin.class.getDeclaredMethod("createMongoClientOptions", PluginTask.class);
        createMongoClientOptions.setAccessible(true);
        MongoClientOptions mongoClientOptions = (MongoClientOptions) createMongoClientOptions.invoke(plugin, task);
        assertThat(false, is(mongoClientOptions.isSslEnabled()));
        assertThat(false, is(mongoClientOptions.isSslInvalidHostNameAllowed()));
    }

    @Test
    public void testResume()
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        final Schema schema = getFieldSchema();
        plugin.resume(task.toTaskSource(), schema, 0, new InputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount)
            {
                return emptyTaskReports(taskCount);
            }
        });
        // no errors happens
    }

    @Test
    public void testCleanup()
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        Schema schema = getFieldSchema();
        plugin.cleanup(task.dump(), schema, 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test
    public void testGuess()
    {
        plugin.guess(config); // no errors happens
    }

    @Test
    public void testRun() throws Exception
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        plugin.transaction(config, new Control());
        assertValidRecords(getFieldSchema(), output);
    }

    @Test
    public void testRunWithLimit() throws Exception
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("limit", 1);
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        plugin.transaction(config, new Control());
        assertValidRecords(getFieldSchema(), output, 1, 0);
    }

    @Test
    public void testRunWithLimitSkip() throws Exception
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("limit", 3)
                .set("skip", 1);
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        plugin.transaction(config, new Control());
        assertValidRecords(getFieldSchema(), output, 3, 1);
    }

    @Test
    public void testRunWithConnectionParams() throws Exception
    {
        MongoClientURI uri = new MongoClientURI(mongoUri);
        String host = uri.getHosts().get(0);
        Integer port = (host.split(":")[1] != null) ? Integer.valueOf(host.split(":")[1]) : 27017;
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("hosts", Arrays.asList(ImmutableMap.of("host", host.split(":")[0], "port", port)))
                .set("user", uri.getUsername())
                .set("password", uri.getPassword())
                .set("database", uri.getDatabase())
                .set("collection", mongoCollection);
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        plugin.transaction(config, new Control());
        assertValidRecords(getFieldSchema(), output);
    }

    @Test
    public void testRunWithIncrementalLoad() throws Exception
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("incremental_field", Optional.of(Arrays.asList("int32_field")));
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        ConfigDiff diff = plugin.transaction(config, new Control());
        ConfigDiff lastRecord = diff.getNested("last_record");

        assertEquals("5", lastRecord.get(String.class, "int32_field"));
    }

    @Test
    public void testRunWithLimitIncrementalLoad() throws Exception
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("id_field_name", "int32_field")
                .set("incremental_field", Optional.of(Arrays.asList("int32_field", "double_field", "datetime_field", "boolean_field")))
                .set("limit", 1);
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        ConfigDiff diff = plugin.transaction(config, new Control());
        ConfigDiff lastRecord = diff.getNested("last_record");

        assertEquals("1", lastRecord.get(String.class, "int32_field"));
        assertEquals("1.23", lastRecord.get(String.class, "double_field"));
        assertEquals("{$date=2015-01-27T10:23:49.000Z}", lastRecord.get(Map.class, "datetime_field").toString());
        assertEquals("true", lastRecord.get(String.class, "boolean_field"));
    }

    @Test
    public void testRunWithLimitIncrementalLoadWithNoRecord() throws Exception
    {
        Map<String, Object> previousLastRecord = new HashMap<>();
        previousLastRecord.put("int32_field", 1);
        previousLastRecord.put("datetime_field", "{$date=2015-01-27T10:23:49.000Z}");
        previousLastRecord.put("boolean_field", true);
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("id_field_name", "int32_field")
                .set("query", "{\"double_field\":{\"$gte\": 1.23}}")
                .set("incremental_field", Optional.of(Arrays.asList("int32_field", "datetime_field", "boolean_field")))
                .set("last_record", previousLastRecord);

        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        ConfigDiff diff = plugin.transaction(config, new Control());
        ConfigDiff lastRecord = diff.getNested("last_record");

        assertEquals("1", lastRecord.get(String.class, "int32_field"));
        assertEquals("{$date=2015-01-27T10:23:49.000Z}", lastRecord.get(String.class, "datetime_field"));
        assertEquals("true", lastRecord.get(String.class, "boolean_field"));
    }

    @Test(expected = ConfigException.class)
    public void testRunWithIncrementalLoadUnsupportedType() throws Exception
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("incremental_field", Optional.of(Arrays.asList("document_field")));
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        plugin.transaction(config, new Control());
    }

    @Test(expected = ValueCodec.UnknownTypeFoundException.class)
    public void testRunWithUnsupportedType() throws Exception
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("stop_on_invalid_record", true);

        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);

        List<Document> documents = new ArrayList<>();
        documents.add(
            new Document("invalid_field", new BsonMaxKey())
        );
        insertDocument(task, documents);

        plugin.transaction(config, new Control());
    }

    @Test
    public void testRunWithAggregation() throws Exception
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("id_field_name", "int32_field")
                .set("aggregation", "{ $match: {\"int32_field\":{\"$gte\":5 },} }");

        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        plugin.transaction(config, new Control());
        assertValidRecordsForAggregation(getFieldSchema(), output);
    }

    @Test
    public void testNormalize() throws Exception
    {
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        ValueCodec codec = new ValueCodec(true, task);

        Method normalize = ValueCodec.class.getDeclaredMethod("normalize", String.class);
        normalize.setAccessible(true);
        assertEquals("_id", normalize.invoke(codec, "_id").toString());
        assertEquals("f1", normalize.invoke(codec, "f1").toString());
    }

    @Test
    public void testNormlizeWithIdFieldName() throws Exception
    {
        ConfigSource config = config().set("id_field_name", "object_id");

        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        ValueCodec codec = new ValueCodec(true, task);

        Method normalize = ValueCodec.class.getDeclaredMethod("normalize", String.class);
        normalize.setAccessible(true);
        assertEquals("object_id", normalize.invoke(codec, "_id").toString());
        assertEquals("f1", normalize.invoke(codec, "f1").toString());
    }

    @Test
    public void testValidateJsonField() throws Exception
    {
        Method validate = MongodbInputPlugin.class.getDeclaredMethod("validateJsonField", String.class, String.class);
        validate.setAccessible(true);
        String invalidJsonString = "{\"name\": invalid}";
        try {
            validate.invoke(plugin, "name", invalidJsonString);
        }
        catch (InvocationTargetException ex) {
            assertEquals(ConfigException.class, ex.getCause().getClass());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBuildIncrementalCondition() throws Exception
    {
        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        Method method = MongodbInputPlugin.class.getDeclaredMethod("buildIncrementalCondition", PluginTask.class);
        method.setAccessible(true);

        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("incremental_field", Optional.of(Arrays.asList("account")));
        task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        Map<String, String> actual = (Map<String, String>) method.invoke(plugin, task);
        Map<String, String> expected = new HashMap<>();
        expected.put("query", "{}");
        expected.put("sort", "{\"account\":1}");
        assertEquals(expected, actual);

        Map<String, Object> lastRecord = new HashMap<>();
        Map<String, String> innerRecord = new HashMap<>();
        innerRecord.put("$oid", "abc");
        lastRecord.put("_id", innerRecord);
        lastRecord.put("int32_field", 15000);
        innerRecord = new HashMap<>();
        innerRecord.put("$date", "2015-01-27T19:23:49Z");
        lastRecord.put("datetime_field", innerRecord);
        config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("query", "{\"double_field\":{\"$gte\": 1.23}}")
                .set("incremental_field", Optional.of(Arrays.asList("_id", "int32_field", "datetime_field")))
                .set("last_record", Optional.of(lastRecord));
        task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        actual = (Map<String, String>) method.invoke(plugin, task);
        expected.put("query", "{\"double_field\":{\"$gte\":1.23},\"int32_field\":{\"$gt\":15000},\"_id\":{\"$gt\":{\"$oid\":\"abc\"}},\"datetime_field\":{\"$gt\":{\"$date\":\"2015-01-27T19:23:49Z\"}}}");
        expected.put("sort", "{\"_id\":1,\"int32_field\":1,\"datetime_field\":1}");
        assertEquals(expected, actual);
    }

    @Test
    public void testBuildIncrementalConditionFieldDuplicated() throws Exception
    {
        Map<String, Object> lastRecord = new HashMap<>();
        lastRecord.put("double_field", "0");

        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("query", "{\"double_field\":{\"$gte\": 1.23}}")
                .set("incremental_field", Optional.of(Arrays.asList("double_field")))
                .set("last_record", Optional.of(lastRecord));
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);
        insertDocument(task, createValidDocuments());

        Method method = MongodbInputPlugin.class.getDeclaredMethod("buildIncrementalCondition", PluginTask.class);
        method.setAccessible(true);
        try {
            method.invoke(plugin, task); // field declaration was duplicated between query and incremental_field
        }
        catch (Exception ex) {
            assertEquals(ConfigException.class, ex.getCause().getClass());
        }
    }

    @Test
    public void testBuildIncrementalConditionFieldRequired() throws Exception
    {
        Map<String, Object> lastRecord = new HashMap<>();
        lastRecord.put("double_field", "0");

        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection)
                .set("incremental_field", Optional.of(Arrays.asList("invalid_field")))
                .set("last_record", Optional.of(lastRecord));
        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        dropCollection(task, mongoCollection);
        createCollection(task, mongoCollection);

        Method method = MongodbInputPlugin.class.getDeclaredMethod("buildIncrementalCondition", PluginTask.class);
        method.setAccessible(true);
        try {
            method.invoke(plugin, task); // field declaration was not set at incremental_field
        }
        catch (Exception ex) {
            assertEquals(ConfigException.class, ex.getCause().getClass());
        }
    }

    static List<TaskReport> emptyTaskReports(int taskCount)
    {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(CONFIG_MAPPER_FACTORY.newTaskReport());
        }
        return reports.build();
    }

    private class Control
            implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(plugin.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }

    private ConfigSource config()
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("uri", mongoUri)
                .set("collection", mongoCollection);
    }

    private ConfigSource configForAuth()
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
            .set("database", "db")
            .set("collection", mongoCollection)
            .set("user", "abcde")
            .set("password", "passw0rd");
    }

    private List<Document> createValidDocuments() throws Exception
    {
        DateFormat format = getUTCDateFormat();

        List<Document> documents = new ArrayList<>();
        documents.add(
            new Document("double_field", 1.23)
                    .append("string_field", "embulk")
                    .append("array_field", Arrays.asList(1, 2, 3))
                    .append("binary_field", new BsonBinary(("test").getBytes("UTF-8")))
                    .append("boolean_field", true)
                    .append("datetime_field", format.parse("2015-01-27T10:23:49.000Z"))
                    .append("null_field", null)
                    .append("regex_field", new BsonRegularExpression(".+?"))
                    .append("javascript_field", new BsonJavaScript("var s = \"javascript\";"))
                    .append("int32_field", 1)
                    .append("timestamp_field", new BsonTimestamp(1463991177, 4))
                    .append("int64_field", new BsonInt64(314159265))
                    .append("document_field", new Document("k", true))
                    .append("symbol_field", new Symbol("symbol"))
        );

        documents.add(
            new Document("boolean_field", false)
                    .append("int32_field", 2)
                    .append("document_field", new Document("k", 1))
        );

        documents.add(
            new Document("int32_field", 3)
                    .append("document_field", new Document("k", 1.23))
        );

        documents.add(
            new Document("int32_field", 4)
                    .append("document_field", new Document("k", "v"))
        );

        documents.add(
            new Document("int32_field", 5)
                    .append("document_field", new Document("k", format.parse("2015-02-02T23:13:45.000Z")))
        );

        return documents;
    }

    private Schema getFieldSchema()
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        columns.add(new Column(0, "record", Types.JSON));
        return new Schema(columns.build());
    }

    private void assertValidRecords(Schema schema, MockPageOutput output) throws Exception
    {
        assertValidRecords(schema, output, 5, 0);
    }

    private void assertValidRecordsForAggregation(Schema schema, MockPageOutput output) throws Exception
    {
        assertValidRecords(schema, output, 1, 4);
    }

    private void assertValidRecords(Schema schema, MockPageOutput output, int limit, int skip) throws Exception
    {
        int maxRecordSize = 5;
        int actualRecordSize = Math.min(maxRecordSize - skip, limit);
        List<Object[]> records = Pages.toObjects(schema, output.pages);
        assertEquals(actualRecordSize, records.size());

        ObjectMapper mapper = new ObjectMapper();
        mapper.setDateFormat(getUTCDateFormat());

        int recordIndex = 0;
        for (int i = skip; i < actualRecordSize; i++) {
            if (i == 0) {
                JsonNode node = mapper.readTree(records.get(recordIndex)[0].toString());
                assertThat(1.23, is(node.get("double_field").asDouble()));
                assertEquals("embulk", node.get("string_field").asText());
                assertEquals("[1,2,3]", node.get("array_field").toString());
                assertEquals("test", node.get("binary_field").asText());
                assertEquals(true, node.get("boolean_field").asBoolean());
                assertEquals("2015-01-27T10:23:49.000Z", node.get("datetime_field").asText());
                assertEquals("null", node.get("null_field").asText());
                assertEquals("BsonRegularExpression{pattern='.+?', options=''}", node.get("regex_field").asText());
                assertEquals("var s = \"javascript\";", node.get("javascript_field").asText());
                assertEquals(1, node.get("int32_field").asLong());
                assertEquals("1463991177", node.get("timestamp_field").asText());
                assertEquals(314159265L, node.get("int64_field").asLong());
                assertEquals("{\"k\":true}", node.get("document_field").toString());
                assertEquals("symbol", node.get("symbol_field").asText());
            }

            if (i == 1) {
                JsonNode node = mapper.readTree(records.get(recordIndex)[0].toString());
                assertEquals(false, node.get("boolean_field").asBoolean());
                assertEquals("{\"k\":1}", node.get("document_field").toString());
            }

            if (i == 2) {
                JsonNode node = mapper.readTree(records.get(recordIndex)[0].toString());
                assertEquals("{\"k\":1.23}", node.get("document_field").toString());
            }

            if (i == 3) {
                JsonNode node = mapper.readTree(records.get(recordIndex)[0].toString());
                assertEquals("{\"k\":\"v\"}", node.get("document_field").toString());
            }

            if (i == 4) {
                JsonNode node = mapper.readTree(records.get(recordIndex)[0].toString());
                assertEquals("{\"k\":\"2015-02-02T23:13:45.000Z\"}", node.get("document_field").toString());
            }
            recordIndex++;
        }
    }

    private void createCollection(PluginTask task, String collectionName) throws Exception
    {
        Method method = MongodbInputPlugin.class.getDeclaredMethod("connect", PluginTask.class);
        method.setAccessible(true);
        MongoDatabase db = (MongoDatabase) method.invoke(plugin, task);
        db.createCollection(collectionName);
    }

    private void dropCollection(PluginTask task, String collectionName) throws Exception
    {
        Method method = MongodbInputPlugin.class.getDeclaredMethod("connect", PluginTask.class);
        method.setAccessible(true);
        MongoDatabase db = (MongoDatabase) method.invoke(plugin, task);
        MongoCollection collection = db.getCollection(collectionName);
        collection.drop();
    }

    private void insertDocument(PluginTask task, List<Document> documents) throws Exception
    {
        Method method = MongodbInputPlugin.class.getDeclaredMethod("connect", PluginTask.class);
        method.setAccessible(true);
        MongoDatabase db = (MongoDatabase) method.invoke(plugin, task);
        MongoCollection collection = db.getCollection(task.getCollection());
        collection.insertMany(documents);
    }

    private DateFormat getUTCDateFormat()
    {
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", java.util.Locale.ENGLISH);
      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
      return dateFormat;
    }
}
