package org.embulk.input.mongodb;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.Task;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.SchemaConfig;

import javax.validation.constraints.Min;

import java.util.List;
import java.util.Map;

public interface PluginTask
        extends Task
{
    // MongoDB connection string URI
    @Config("uri")
    @ConfigDefault("null")
    Optional<String> getUri();

    @Config("hosts")
    @ConfigDefault("null")
    Optional<List<HostTask>> getHosts();

    @Config("user")
    @ConfigDefault("null")
    Optional<String> getUser();

    @Config("password")
    @ConfigDefault("null")
    Optional<String> getPassword();

    @Config("database")
    @ConfigDefault("null")
    Optional<String> getDatabase();

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
    void setQuery(String query);

    @Config("aggregation")
    @ConfigDefault("null")
    Optional<String> getAggregation();

    @Config("sort")
    @ConfigDefault("\"{}\"")
    String getSort();
    void setSort(String sort);

    @Config("limit")
    @ConfigDefault("null")
    Optional<Integer> getLimit();

    @Config("skip")
    @ConfigDefault("null")
    Optional<Integer> getSkip();

    @Config("id_field_name")
    @ConfigDefault("\"_id\"")
    String getIdFieldName();

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

    @Config("incremental_field")
    @ConfigDefault("null")
    Optional<List<String>> getIncrementalField();

    @Config("last_record")
    @ConfigDefault("null")
    Optional<Map<String, Object>> getLastRecord();

    @ConfigInject
    BufferAllocator getBufferAllocator();
}
