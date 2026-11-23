package dev.thebooleanguy.dbpool;

import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

public class DBConfig {
    private final String url;
    private final String username;
    private final String password;

    public DBConfig() throws IOException {
        Properties dbProps = new Properties();

        try (InputStream dbConfig = getClass().getClassLoader().getResourceAsStream("db.properties")) {
            if (dbConfig == null) {
                throw new IOException("db.properties not found on classpath");
            }
            dbProps.load(dbConfig);
        }

        url = dbProps.getProperty("db.url");
        username = dbProps.getProperty("db.username");
        password = dbProps.getProperty("db.password");

        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("url cannot be empty");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("username cannot be empty");
        }
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
