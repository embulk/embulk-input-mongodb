package org.embulk.input.mongodb;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface HostTask
        extends Task
{
    @Config("host")
    String getHost();

    @Config("port")
    @ConfigDefault("27017")
    int getPort();
}
