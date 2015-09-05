package org.embulk.input.mongodb;

import com.google.common.base.Optional;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;

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
        SchemaConfig getFields();

        @Config("query")
        @ConfigDefault("\"{}\"")
        String getQuery();

        @Config("sort")
        @ConfigDefault("\"{}\"")
        String getSort();

        @Config("batch_size")
        @ConfigDefault("10000")
        Optional<Integer> getBatchSize();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema schema = task.getFields().toSchema();
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
        SchemaConfig fields = task.getFields();
        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        MongoDatabase db = connect(task);
        MongoCollection<Document> collection = db.getCollection(task.getCollection());

        StringBuilder proj = new StringBuilder("[");
        for (ColumnConfig c : fields.getColumns()) {
            c.getName();
        }
        proj.append("}");

        Bson query = (Bson) JSON.parse(task.getQuery());
        Bson sort = (Bson) JSON.parse(task.getSort());

        FindIterable<Document> iterable = collection
                                        .find(query)
                                        .sort(sort)
                                        .batchSize(task.getBatchSize().get());

        MongoCursor<Document> cursor = iterable.iterator();
        try {
            while (cursor.hasNext()) {
                fetch(cursor, pageBuilder);
            }
        } finally {
            cursor.close();
        }

        pageBuilder.finish();

        TaskReport report = Exec.newTaskReport();
        return report;
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private MongoDatabase connect(PluginTask task) {
        MongoClientURI uri = new MongoClientURI(task.getUri());
        MongoClient mongoClient = new MongoClient(uri);
        return mongoClient.getDatabase(uri.getDatabase());
    }

    private void fetch(MongoCursor<Document> cursor, PageBuilder pageBuilder) {
        Document doc = cursor.next();
        List<Column> columns = pageBuilder.getSchema().getColumns();
        for (Column c : columns) {
            Type t = c.getType();
            String key = c.getName();

            // 'id' is special alias key name of MongoDB ObjectId
            // http://docs.mongodb.org/manual/reference/object-id/
            if (key.equals("id")) {
                key = "_id";
            }

            if (!doc.containsKey(key) || doc.get(key) == null) {
                pageBuilder.setNull(c);
            } else {
                switch (t.getName()) {
                case "boolean":
                    pageBuilder.setBoolean(c, doc.getBoolean(key));
                    break;

                case "long":
                    // MongoDB can contain both 'int' and 'long', but embulk only support 'long'
                    // So enable handling both 'int' and 'long', first get value as java.lang.Number, then convert it to long
                    pageBuilder.setLong(c, ((Number) doc.get(key)).longValue());
                    break;

                case "double":
                    pageBuilder.setDouble(c, doc.getDouble(key));
                    break;

                case "string":
                // Enable output object like ObjectId as string, this is reason I don't use doc.getString(key).
                pageBuilder.setString(c, doc.get(key).toString());
                break;

                case "timestamp":
                    pageBuilder.setTimestamp(c, Timestamp.ofEpochMilli(doc.getDate(key).getTime()));
                }
            }
        }
        pageBuilder.addRecord();
    }
}
