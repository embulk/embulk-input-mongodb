package org.embulk.input.mongodb;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface HostTask
        extends Task
{
    @Config("host")
    String getHost();

    @Config("port")
    @ConfigDefault("27017")
    int getPort();
}
