# simple-dbpool

A small Java 17 JDBC connection pool for learning and experimentation. It creates connections on demand, reuses returned connections, and limits the number of physical connections it opens.

## Features

- Lazy connection creation through `DriverManager`
- Configurable maximum number of physical connections
- Reuse of idle connections and tracking of borrowed connections
- A one-second wait when the pool is at capacity
- Graceful shutdown: idle connections close immediately; borrowed connections close when returned
- `awaitTermination()` for waiting until shutdown has completed

## Requirements

- JDK 17 or newer
- Maven
- A JDBC driver for the database in use

The project includes H2 for its in-memory demo. The pool itself uses standard JDBC APIs and can work with another database when that database's driver is on the application classpath.

## Use It

Import `dev.thebooleanguy.dbpool.DBPool`, `java.sql.Connection`, `java.sql.PreparedStatement`, `java.sql.ResultSet`, and `java.util.concurrent.TimeUnit`. Place this code in a method that handles `SQLException` and `InterruptedException`:

```java
DBPool pool = new DBPool(
        4,
        "jdbc:h2:mem:app;DB_CLOSE_DELAY=-1",
        "sa",
        "");

Connection connection = null;
try {
    connection = pool.getConnection();

    try (PreparedStatement statement = connection.prepareStatement("SELECT 1");
         ResultSet results = statement.executeQuery()) {
        if (results.next()) {
            System.out.println(results.getInt(1));
        }
    }
} finally {
    if (connection != null) {
        pool.releaseConnection(connection);
    }
}

pool.shutdown();
if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
    System.err.println("Pool did not terminate in time");
}
```

Return every borrowed connection in a `finally` block. Do not call `Connection.close()` to return a connection; the pool's `releaseConnection()` method handles returning it while the pool is running, or closing it during shutdown.

## Lifecycle

1. Construct the pool with a positive maximum, JDBC URL, username, and password. Construction does not open a database connection.
2. `getConnection()` creates a physical connection when capacity is available, or reuses an idle one. If all connections are borrowed, it waits up to one second and then throws `SQLException` if no connection becomes available. The call can also throw `InterruptedException`.
3. Call `releaseConnection(connection)` when finished with a borrowed connection.
4. Call `shutdown()` to reject new acquisitions and close idle connections. Connections still borrowed are closed as they are released.
5. After shutdown, `awaitTermination(timeout, unit)` returns whether all pool connections have been closed. It returns `false` if the pool is still running or if the timeout expires before termination.

## Build and Test

Run the test suite:

```bash
mvn test
```

Build the package:

```bash
mvn clean package
```

Pool tests use a test-only JDBC driver with controlled mock connections, so they do not require a running database.