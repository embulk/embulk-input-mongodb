package org.embulk.input.mongodb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonBinary;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonMaxKey;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.mongodb.MongodbInputPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestMongodbInputPlugin {
    private static String MONGO_URI;
    private static String MONGO_COLLECTION;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ConfigSource config;
    private MongodbInputPlugin plugin;
    private MockPageOutput output;

    /*
     * This test case requires environment variables
     *   MONGO_URI
     *   MONGO_COLLECTION
     */
    @BeforeClass
    public static void initializeConstant() {
        MONGO_URI = System.getenv("MONGO_URI");
        MONGO_COLLECTION = System.getenv("MONGO_COLLECTION");
    }

    @Before
    public void createResources() throws Exception {
        config = config();
        plugin = new MongodbInputPlugin();
        output = new MockPageOutput();
    }

    @Test
    public void checkDefaultValues() {
        ConfigSource config = Exec.newConfigSource()
                .set("uri", MONGO_URI)
                .set("collection", MONGO_COLLECTION);

        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals("{}", task.getQuery());
        assertEquals("{}", task.getSort());
        assertEquals((long) 10000, (long) task.getBatchSize());
        assertEquals("record", task.getJsonColumnName());
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesUriIsNull() {
        ConfigSource config = Exec.newConfigSource()
                .set("uri", null)
                .set("collection", MONGO_COLLECTION);

        plugin.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesInvalidUri()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("uri", "mongodb://mongouser:password@non-exists.example.com:23490/test")
                .set("collection", MONGO_COLLECTION);

        plugin.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesCollectionIsNull() {
        ConfigSource config = Exec.newConfigSource()
                .set("uri", MONGO_URI)
                .set("collection", null);

        plugin.transaction(config, new Control());
    }

    @Test
    public void testResume() {
        PluginTask task = config.loadConfig(PluginTask.class);
        final Schema schema = getFieldSchema();
        plugin.resume(task.dump(), schema, 0, new InputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount) {
                return emptyTaskReports(taskCount);
            }
        });
        // no errors happens
    }

    @Test
    public void testCleanup() {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema schema = getFieldSchema();
        plugin.cleanup(task.dump(), schema, 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test
    public void testGuess() {
        plugin.guess(config); // no errors happens
    }

    @Test
    public void testRun() throws Exception {
        PluginTask task = config.loadConfig(PluginTask.class);

        dropCollection(task, MONGO_COLLECTION);
        createCollection(task, MONGO_COLLECTION);
        insertDocument(task, createValidDocuments());

        plugin.transaction(config, new Control());
        assertValidRecords(getFieldSchema(), output);
    }

    @Test(expected = ValueCodec.UnknownTypeFoundException.class)
    public void testRunWithUnsupportedType() throws Exception {
        ConfigSource config = Exec.newConfigSource()
                .set("uri", MONGO_URI)
                .set("collection", MONGO_COLLECTION)
                .set("stop_on_invalid_record", true);

        PluginTask task = config.loadConfig(PluginTask.class);

        dropCollection(task, MONGO_COLLECTION);
        createCollection(task, MONGO_COLLECTION);

        List<Document> documents = new ArrayList<>();
        documents.add(
            new Document("invalid_field", new BsonMaxKey())
        );
        insertDocument(task, documents);

        plugin.transaction(config, new Control());
    }

    @Test
    public void testNormalize() throws Exception {
        ValueCodec codec = new ValueCodec(true);

        Method normalize = ValueCodec.class.getDeclaredMethod("normalize", String.class, boolean.class);
        normalize.setAccessible(true);
        assertEquals("_id", normalize.invoke(codec, "id", true).toString());
        assertEquals("_id", normalize.invoke(codec, "_id", true).toString());
        assertEquals("f1", normalize.invoke(codec, "f1", true).toString());

        assertEquals("id", normalize.invoke(codec, "id", false).toString());
        assertEquals("_id", normalize.invoke(codec, "_id", false).toString());
        assertEquals("f1", normalize.invoke(codec, "f1", false).toString());
    }

    @Test
    public void testValidateJsonField() throws Exception {
        Method validate = MongodbInputPlugin.class.getDeclaredMethod("validateJsonField", String.class, String.class);
        validate.setAccessible(true);
        String invalidJsonString = "{\"name\": invalid}";
        try {
            validate.invoke(plugin, "name", invalidJsonString);
        } catch (InvocationTargetException ex) {
            assertEquals(ConfigException.class, ex.getCause().getClass());
        }
    }

    static List<TaskReport> emptyTaskReports(int taskCount) {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(Exec.newTaskReport());
        }
        return reports.build();
    }

    private class Control
            implements InputPlugin.Control {
        @Override
        public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount) {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(plugin.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }

    private ConfigSource config() {
        return Exec.newConfigSource()
                .set("uri", MONGO_URI)
                .set("collection", MONGO_COLLECTION)
                .set("last_path", "");
    }

    private List<Document> createValidDocuments() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

        List<Document> documents = new ArrayList<>();
        documents.add(
            new Document("double_field", 1.23)
                    .append("string_field", "embulk")
                    .append("array_field", Arrays.asList(1,2,3))
                    .append("binary_field", new BsonBinary(("test").getBytes("UTF-8")))
                    .append("boolean_field", true)
                    .append("datetime_field", format.parse("2015-01-27T19:23:49Z"))
                    .append("null_field", null)
                    .append("regex_field", new BsonRegularExpression(".+?"))
                    .append("javascript_field", new BsonJavaScript("var s = \"javascript\";"))
                    .append("int32_field", 32864)
                    .append("timestamp_field", new BsonTimestamp(1463991177, 4))
                    .append("int64_field", new BsonInt64(314159265))
                    .append("document_field", new Document("k", true))
        );

        documents.add(
            new Document("boolean_field", false)
                    .append("document_field", new Document("k", 1))
        );

        documents.add(new Document("document_field", new Document("k", 1.23)));

        documents.add(new Document("document_field", new Document("k", "v")));

        documents.add(new Document("document_field", new Document("k", format.parse("2015-02-03T08:13:45Z"))));

        return documents;
    }

    private Schema getFieldSchema() {
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        columns.add(new Column(0, "record", Types.JSON));
        return new Schema(columns.build());
    }

    private void assertValidRecords(Schema schema, MockPageOutput output) throws Exception {
        List<Object[]> records = Pages.toObjects(schema, output.pages);
        assertEquals(5, records.size());

        ObjectMapper mapper = new ObjectMapper();

        {
            JsonNode node = mapper.readTree(records.get(0)[0].toString());
            assertThat(1.23, is(node.get("double_field").asDouble()));
            assertEquals("embulk", node.get("string_field").asText());
            assertEquals("[1,2,3]", node.get("array_field").toString());
            assertEquals("test", node.get("binary_field").asText());
            assertEquals(true, node.get("boolean_field").asBoolean());
            assertEquals("2015-01-27T10:23:49.000Z", node.get("datetime_field").asText());
            assertEquals("null", node.get("null_field").asText());
            assertEquals("BsonRegularExpression{pattern='.+?', options=''}", node.get("regex_field").asText());
            assertEquals("var s = \"javascript\";", node.get("javascript_field").asText());
            assertEquals(32864L, node.get("int32_field").asLong());
            assertEquals("1463991177", node.get("timestamp_field").asText());
            assertEquals(314159265L, node.get("int64_field").asLong());
            assertEquals("{\"k\":true}", node.get("document_field").toString());
        }

        {
            JsonNode node = mapper.readTree(records.get(1)[0].toString());
            assertEquals(false, node.get("boolean_field").asBoolean());
            assertEquals("{\"k\":1}", node.get("document_field").toString());
        }

        {
            JsonNode node = mapper.readTree(records.get(2)[0].toString());
            assertEquals("{\"k\":1.23}", node.get("document_field").toString());
        }

        {
            JsonNode node = mapper.readTree(records.get(3)[0].toString());
            assertEquals("{\"k\":\"v\"}", node.get("document_field").toString());
        }

        {
            JsonNode node = mapper.readTree(records.get(4)[0].toString());
            assertEquals("{\"k\":\"2015-02-02T23:13:45.000Z\"}", node.get("document_field").toString());
        }
    }

    private void createCollection(PluginTask task, String collectionName) throws Exception {
        Method method = MongodbInputPlugin.class.getDeclaredMethod("connect", PluginTask.class);
        method.setAccessible(true);
        MongoDatabase db = (MongoDatabase) method.invoke(plugin, task);
        db.createCollection(collectionName);
    }

    private void dropCollection(PluginTask task, String collectionName) throws Exception {
        Method method = MongodbInputPlugin.class.getDeclaredMethod("connect", PluginTask.class);
        method.setAccessible(true);
        MongoDatabase db = (MongoDatabase) method.invoke(plugin, task);
        MongoCollection collection = db.getCollection(collectionName);
        collection.drop();
    }

    private void insertDocument(PluginTask task, List<Document> documents) throws Exception {
        Method method = MongodbInputPlugin.class.getDeclaredMethod("connect", PluginTask.class);
        method.setAccessible(true);
        MongoDatabase db = (MongoDatabase) method.invoke(plugin, task);
        MongoCollection collection = db.getCollection(task.getCollection());
        collection.insertMany(documents);
    }
}
