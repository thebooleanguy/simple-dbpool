package dev.thebooleanguy.dbpool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An educational microbenchmark; its results are not a rigorous performance
 * comparison.
 */
public final class DBPoolBenchmark {
    private static final String QUERY = "SELECT 1";
    private static final int POOL_CAPACITY = 5;
    private static final int DEFAULT_ITERATIONS = 1_000;
    private static final int DEFAULT_THREADS = 5;
    private static final int MAX_WARMUP_OPERATIONS = 100;

    private DBPoolBenchmark() {
    }

    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
        if (args.length > 3) {
            throw new IllegalArgumentException("Usage: DBPoolBenchmark [iterations] [threads] [warmup]");
        }

        int iterations = parseCount(args, 0, DEFAULT_ITERATIONS, "iterations", false);
        int threads = parseCount(args, 1, DEFAULT_THREADS, "threads", false);
        int warmupOperations = parseCount(args, 2, Math.min(MAX_WARMUP_OPERATIONS, iterations), "warmup", true);
        DBConfig config = new DBConfig();
        Properties connectionProperties = createConnectionProperties(config);

        System.out.println("Educational JDBC microbenchmark (not a rigorous performance benchmark)");
        System.out.printf("Usage: DBPoolBenchmark [iterations=%d] [threads=%d] [warmup=%d]%n",
                DEFAULT_ITERATIONS, DEFAULT_THREADS, Math.min(MAX_WARMUP_OPERATIONS, iterations));
        System.out.printf(
                "Iterations: %d | Warmup operations per scenario: %d | Concurrent threads: %d | Pool capacity: %d%n",
                iterations, warmupOperations, threads, POOL_CAPACITY);
        System.out.println("Assumptions: all scenarios execute SELECT 1; measured totals exclude setup and warmup.");
        System.out.println(
                "Concurrent total includes task submission and waiting; average is per-task latency, including pool wait and release.");
        System.out.println();

        BenchmarkResult directWarmup = runDirect(config, connectionProperties, warmupOperations);
        BenchmarkResult direct = runDirect(config, connectionProperties, iterations);
        BenchmarkResult pooledWarmup;
        BenchmarkResult pooled;
        DBPool pool = new DBPool(POOL_CAPACITY, config);
        try {
            pooledWarmup = runPooled(pool, warmupOperations);
            pooled = runPooled(pool, iterations);
        } finally {
            shutdownPool(pool);
        }

        BenchmarkResult concurrentWarmup;
        BenchmarkResult concurrent;
        DBPool concurrentPool = new DBPool(POOL_CAPACITY, config);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            concurrentWarmup = runConcurrent(concurrentPool, executor, warmupOperations);
            concurrent = runConcurrent(concurrentPool, executor, iterations);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Benchmark executor did not terminate within 60 seconds");
                    }
                }
            } catch (InterruptedException interrupted) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
                throw interrupted;
            } finally {
                shutdownPool(concurrentPool);
            }
        }

        System.out.printf("%-28s %8s %8s %12s %12s %14s%n",
                "Scenario", "Ops", "Failures", "Total (ms)", "Avg op (ms)", "Ops / sec");
        System.out.println("-".repeat(88));
        printResult("Direct JDBC", direct);
        printResult("Connection pool", pooled);
        printResult("Concurrent pool", concurrent);

        printWarmupFailures("Direct JDBC", directWarmup);
        printWarmupFailures("Connection pool", pooledWarmup);
        printWarmupFailures("Concurrent pool", concurrentWarmup);
    }

    private static int parseCount(String[] args, int index, int defaultValue, String name,
            boolean allowZero) {
        if (args.length <= index) {
            return defaultValue;
        }
        int value;
        try {
            value = Integer.parseInt(args[index]);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " must be an integer", exception);
        }
        if (value < 0 || (!allowZero && value == 0)) {
            throw new IllegalArgumentException(name + " must be " + (allowZero ? "non-negative" : "positive"));
        }
        return value;
    }

    private static Properties createConnectionProperties(DBConfig config) {
        Properties properties = new Properties();
        properties.setProperty("user", config.getUsername());
        if (config.getPassword() != null) {
            properties.setProperty("password", config.getPassword());
        }
        return properties;
    }

    private static BenchmarkResult runDirect(DBConfig config, Properties properties, int operations) {
        BenchmarkResult result = new BenchmarkResult(operations);
        long start = System.nanoTime();
        for (int operation = 0; operation < operations; operation++) {
            result.record(runDirectOperation(config, properties));
        }
        result.totalNanos = System.nanoTime() - start;
        return result;
    }

    private static OperationResult runDirectOperation(DBConfig config, Properties properties) {
        long start = System.nanoTime();
        boolean successful = false;
        String failure = null;
        try (Connection connection = DriverManager.getConnection(config.getUrl(), properties)) {
            executeQuery(connection);
            successful = true;
        } catch (SQLException exception) {
            failure = exception.getMessage();
        }
        return new OperationResult(successful, System.nanoTime() - start, failure);
    }

    private static BenchmarkResult runPooled(DBPool pool, int operations) throws InterruptedException {
        BenchmarkResult result = new BenchmarkResult(operations);
        long start = System.nanoTime();
        for (int operation = 0; operation < operations; operation++) {
            result.record(runPooledOperation(pool));
        }
        result.totalNanos = System.nanoTime() - start;
        return result;
    }

    private static BenchmarkResult runConcurrent(DBPool pool, ExecutorService executor, int operations)
            throws InterruptedException {
        long start = System.nanoTime();
        List<Future<OperationResult>> tasks = new ArrayList<>(operations);
        for (int operation = 0; operation < operations; operation++) {
            tasks.add(executor.submit(() -> runPooledOperation(pool)));
        }

        BenchmarkResult result = new BenchmarkResult(operations);
        for (Future<OperationResult> task : tasks) {
            try {
                result.record(task.get());
            } catch (ExecutionException exception) {
                Throwable cause = exception.getCause();
                result.record(new OperationResult(false, 0, cause.getMessage()));
            }
        }
        result.totalNanos = System.nanoTime() - start;
        return result;
    }

    private static OperationResult runPooledOperation(DBPool pool) throws InterruptedException {
        long start = System.nanoTime();
        Connection connection = null;
        boolean successful = false;
        String failure = null;
        try {
            connection = pool.getConnection();
            executeQuery(connection);
            successful = true;
        } catch (SQLException exception) {
            failure = exception.getMessage();
        } finally {
            if (connection != null) {
                pool.releaseConnection(connection);
            }
        }
        return new OperationResult(successful, System.nanoTime() - start, failure);
    }

    private static void executeQuery(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(QUERY)) {
            if (!resultSet.next() || resultSet.getInt(1) != 1) {
                throw new SQLException("SELECT 1 did not return 1");
            }
        }
    }

    private static void shutdownPool(DBPool pool) throws InterruptedException {
        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Pool did not terminate within 30 seconds");
        }
    }

    private static void printResult(String name, BenchmarkResult result) {
        double totalMillis = result.totalNanos / 1_000_000.0;
        String averageMillis = result.successes == 0
                ? "n/a"
                : String.format("%.3f", result.operationNanos / (double) result.successes / 1_000_000.0);
        double operationsPerSecond = result.totalNanos == 0
                ? 0
                : result.successes * 1_000_000_000.0 / result.totalNanos;
        System.out.printf("%-28s %8d %8d %12.2f %12s %14.2f%n",
                name, result.attempted, result.failures, totalMillis, averageMillis, operationsPerSecond);
        if (result.firstFailure != null) {
            System.out.println("  First failure: " + result.firstFailure);
        }
    }

    private static void printWarmupFailures(String name, BenchmarkResult result) {
        if (result.failures > 0) {
            System.out.printf("%s warmup failures: %d (first: %s)%n",
                    name, result.failures, result.firstFailure);
        }
    }

    private static final class BenchmarkResult {
        private final int attempted;
        private int successes;
        private int failures;
        private long operationNanos;
        private long totalNanos;
        private String firstFailure;

        private BenchmarkResult(int attempted) {
            this.attempted = attempted;
        }

        private void record(OperationResult operation) {
            if (operation.successful) {
                successes++;
                operationNanos += operation.durationNanos;
            } else {
                failures++;
                if (firstFailure == null) {
                    firstFailure = operation.failure;
                }
            }
        }
    }

    private static final class OperationResult {
        private final boolean successful;
        private final long durationNanos;
        private final String failure;

        private OperationResult(boolean successful, long durationNanos, String failure) {
            this.successful = successful;
            this.durationNanos = durationNanos;
            this.failure = failure;
        }
    }
}