package dev.thebooleanguy.dbpool;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBPool {
    private enum PoolState {
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }

    private final int maxConnections;
    private int totalConnections = 0;
    private final List<Connection> idleConnections;
    private final Set<Connection> borrowedConnections;
    private final Object lock = new Object();
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private PoolState state = PoolState.RUNNING;

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

    public Connection getConnection() throws SQLException, InterruptedException {
        Connection connection = null;
        boolean createConnection = false;
        boolean closeCreatedConnection = false;

        long waitTimeMs = 1000;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitTimeMs);

        synchronized (lock) {
            while (true) {
                if (state != PoolState.RUNNING) {
                    throw new SQLException("DBPool is not running...");
                }
                long remainingTime = deadline - System.nanoTime();

                if (remainingTime <= 0 && connection == null) {
                    throw new SQLException("No idle connection and capacity is exhausted");
                }
                if (totalConnections >= maxConnections && idleConnections.isEmpty()) {

                    long millis = remainingTime / 1_000_000;
                    long fractionalNanos = remainingTime % 1_000_000;
                    lock.wait(millis, (int) fractionalNanos);

                } else if (!idleConnections.isEmpty()) {
                    connection = idleConnections.remove(0);
                    borrowedConnections.add(connection);
                    break;
                } else if (idleConnections.isEmpty() && totalConnections < maxConnections) {
                    totalConnections++;
                    createConnection = true;
                    break;
                }
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
                    if (state == PoolState.SHUTDOWN && totalConnections == 0) {
                        state = PoolState.TERMINATED;
                        lock.notifyAll();
                    }
                    lock.notifyAll();
                    throw e;
                }
            }
        }

        synchronized (lock) {
            if (state != PoolState.RUNNING) {
                closeCreatedConnection = true;
            } else if (createConnection) {
                borrowedConnections.add(connection);
            }
        }

        if (closeCreatedConnection) {
            try {
                connection.close();
                throw new SQLException("Pool is not running");
            } finally {
                synchronized (lock) {
                    totalConnections--;
                    if (state == PoolState.SHUTDOWN && totalConnections == 0) {
                        state = PoolState.TERMINATED;
                        lock.notifyAll();
                    }
                }
            }

        }

        return connection;
    }

    public void releaseConnection(Connection connection) {
        boolean closeConnection = false;
        synchronized (lock) {
            if (state == PoolState.RUNNING) {
                if (borrowedConnections.remove(connection)) {
                    idleConnections.add(connection);
                    lock.notifyAll();
                }
            } else if (state == PoolState.SHUTDOWN) {
                if (borrowedConnections.remove(connection)) {
                    closeConnection = true;
                }
            }
        }
        if (closeConnection) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("Exception caught closing a connection: " + e.getMessage());
            } finally {
                synchronized (lock) {
                    totalConnections--;

                    if (state == PoolState.SHUTDOWN && totalConnections == 0) {
                        state = PoolState.TERMINATED;
                        lock.notifyAll();
                    }
                }
            }
        }
    }

    public void shutdown() {
        Connection[] cleanupBuffer;
        synchronized (lock) {
            if (state != PoolState.RUNNING) {
                return;
            }
            state = PoolState.SHUTDOWN;
            cleanupBuffer = new Connection[idleConnections.size()];
            cleanupBuffer = idleConnections.toArray(cleanupBuffer);
            idleConnections.clear();
            lock.notifyAll();

            if (totalConnections == 0) {
                state = PoolState.TERMINATED;
                lock.notifyAll();
                return;
            }
        }
        for (Connection connection : cleanupBuffer) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("Exception caught closing a connection: " + e.getMessage());
            } finally {
                synchronized (lock) {
                    totalConnections--;
                    if (state == PoolState.SHUTDOWN && totalConnections == 0) {
                        state = PoolState.TERMINATED;
                        lock.notifyAll();
                    }
                }
            }
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new NullPointerException("Null unit supplied by the caller");
        }
        if (timeout < 0) {
            throw new IllegalArgumentException("Negative timeout supplied by the caller");
        }

        synchronized (lock) {
            if (state == PoolState.RUNNING) {
                return false;
            } else if (state == PoolState.TERMINATED) {
                return true;
            }

            long deadline = System.nanoTime() + unit.toNanos(timeout);
            long remainingTime = deadline - System.nanoTime();

            while (true) {
                if (remainingTime <= 0) {
                    break;
                }

                long millis = remainingTime / 1_000_000;
                long fractionalNanos = remainingTime % 1_000_000;

                if (state != PoolState.SHUTDOWN) {
                    break;
                }
                lock.wait(millis, (int) fractionalNanos);

                remainingTime = deadline - System.nanoTime();
            }

            return state == PoolState.TERMINATED;
        }
    }
}
