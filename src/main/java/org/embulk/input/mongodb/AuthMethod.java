package org.embulk.input.mongodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.embulk.config.ConfigException;

import java.util.Locale;

public enum AuthMethod
{
    AUTO,
    SCRAM_SHA_1,
    MONGODB_CR;

    @JsonValue
    @Override
    public String toString()
    {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static AuthMethod fromString(String value)
    {
        switch (value.replace("_", "-")) {
            case "scram-sha-1":
                return SCRAM_SHA_1;
            case "mongodb-cr":
                return MONGODB_CR;
            case "auto":
                return AUTO;
            default:
                throw new ConfigException(String.format("Unknown auth_method '%s'. Supported auth_method are scram-sha-1, mongodb-cr, auto", value));
        }
    }
}
