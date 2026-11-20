package dev.thebooleanguy.dbpool;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBPool {
    private final int maxConnections;
    private int totalConnections = 0;
    private final List<Connection> idleConnections;
    private final Set<Connection> borrowedConnections;
    private final Object lock = new Object();
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public DBPool(int maxConnections, String jdbcUrl, String username, String password) {
        if (maxConnections <= 0) {
            throw new IllegalArgumentException("maxConnections must be greater than zero");
        }
        this.maxConnections = maxConnections;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;

        this.idleConnections = new ArrayList<Connection>(maxConnections);
        this.borrowedConnections = new HashSet<Connection>(maxConnections);
    }

    public Connection getConnection() throws SQLException {
        Connection connection = null;
        boolean createConnection = false;

        synchronized (lock) {
            if (totalConnections >= maxConnections && idleConnections.isEmpty()) {
                throw new SQLException("No idle connection and capacity is exhausted");
            } else if (!idleConnections.isEmpty()) {
                connection = idleConnections.remove(0);
                borrowedConnections.add(connection);
            } else if (idleConnections.isEmpty() && totalConnections < maxConnections) {
                totalConnections++;
                createConnection = true;
            }
        }

        if (connection != null) {
            return connection;
        } else if (createConnection) {
            try {
                connection = DriverManager.getConnection(jdbcUrl, username, password);
            } catch (SQLException e) {
                synchronized (lock) {
                    totalConnections--;
                    throw e;
                }
            }
        }

        synchronized (lock) {
            if (createConnection) {
                borrowedConnections.add(connection);
            }
        }
        return connection;
    }

    public void releaseConnection(Connection connection) {
        synchronized (lock) {
            if (borrowedConnections.remove(connection)) {
                idleConnections.add(connection);
                lock.notifyAll();
            }
        }
    }
}
