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
- PostgreSQL running with a database available

The PostgreSQL JDBC driver is included in the Maven dependencies. The project also includes H2.

## Database Configuration

`DBConfig` loads `db.properties` from the classpath. Copy `src/main/resources/db.properties.example` to `src/main/resources/db.properties` and set the URL and credentials for your local PostgreSQL instance:

```properties
db.url=jdbc:postgresql://localhost:5432/dbpool_test
db.username=your_username
db.password=your_password
```

The real `db.properties` file is ignored by Git so local credentials are not committed.

## Use It

Import `dev.thebooleanguy.dbpool.DBConfig`, `dev.thebooleanguy.dbpool.DBPool`, `java.sql.Connection`, `java.sql.ResultSet`, `java.sql.Statement`, and `java.util.concurrent.TimeUnit`. Place this code in a method that handles `IOException`, `SQLException`, and `InterruptedException`:

```java
DBConfig config = new DBConfig();
DBPool pool = new DBPool(4, config);
try {
    Connection connection = pool.getConnection();
    try {
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery("SELECT 1")) {
            if (results.next()) {
                System.out.println(results.getInt(1));
            }
        }
    } finally {
        pool.releaseConnection(connection);
    }
} finally {
    pool.shutdown();

    if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
        System.err.println("Pool did not terminate in time");
    }
}
```

Return every borrowed connection in a `finally` block. Do not call `Connection.close()` to return a connection; the pool's `releaseConnection()` method handles returning it while the pool is running, or closing it during shutdown.

## Lifecycle

1. Load `DBConfig` and construct the pool with a positive maximum. Construction does not open a database connection.
2. `getConnection()` creates a physical connection when capacity is available, or reuses an idle one. If all connections are borrowed, it waits up to one second and then throws `SQLException` if no connection becomes available. The call can also throw `InterruptedException`.
3. Call `releaseConnection(connection)` when finished with a borrowed connection.
4. Call `shutdown()` to reject new acquisitions and close idle connections. Connections still borrowed are closed as they are released.
5. After shutdown, `awaitTermination(timeout, unit)` returns whether all pool connections have been closed. It returns `false` if the pool is still running or if the timeout expires before termination.

## Educational Benchmark

`DBPoolBenchmark` compares three `SELECT 1` workloads against the configured database: a new direct JDBC connection per operation, sequential operations using the pool, and concurrent pooled operations using five worker threads and a pool capacity of five. It warms up each scenario and prints total duration, average operation time, operations per second, and failures.

Run the `DBPoolBenchmark` main class from your IDE with optional program arguments:

```text
[iterations] [threads] [warmup]
```

Defaults are 1,000 measured iterations, 5 concurrent threads, and up to 100 warmup operations per scenario. The benchmark is an educational microbenchmark, not a rigorous performance comparison; results depend on the machine, database, network, and workload. The benchmark was built with GitHub Copilot.

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