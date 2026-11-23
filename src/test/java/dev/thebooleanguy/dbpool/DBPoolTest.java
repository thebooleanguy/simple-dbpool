package dev.thebooleanguy.dbpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DBPoolTest {
    private TestDriver driver;
    private final List<DBPool> pools = new ArrayList<>();

    @BeforeEach
    void registerDriver() throws SQLException {
        driver = new TestDriver();
        DriverManager.registerDriver(driver);
    }

    @AfterEach
    void cleanUp() throws SQLException {
        for (DBPool pool : pools) {
            pool.shutdown();
        }
        DriverManager.deregisterDriver(driver);
    }

    /** A released connection is returned to the next borrower unchanged. */
    @Test
    void reusesReleasedConnection() throws Exception {
        DBPool pool = newPool(1);
        Connection first = pool.getConnection();

        pool.releaseConnection(first);

        assertSame(first, pool.getConnection());
        assertEquals(1, driver.connections.size());
    }

    /**
     * A saturated pool times out without creating a connection beyond its limit.
     */
    @Test
    void neverCreatesMoreThanMaximumConnections() throws Exception {
        DBPool pool = newPool(2);
        pool.getConnection();
        pool.getConnection();

        assertThrows(SQLException.class, pool::getConnection);

        assertEquals(2, driver.connections.size());
    }

    /** An exhausted pool reports acquisition failure after its one-second wait. */
    @Test
    void acquisitionTimesOutAfterApproximatelyOneSecond() throws Exception {
        DBPool pool = newPool(1);
        pool.getConnection();
        long start = System.nanoTime();

        assertThrows(SQLException.class, pool::getConnection);

        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(elapsedMillis >= 900, "Timed out too early: " + elapsedMillis + " ms");
        assertTrue(elapsedMillis < 2500, "Timed out too late: " + elapsedMillis + " ms");
    }

    /** A waiter receives the released connection instead of timing out. */
    @Test
    void waitingBorrowerReceivesReleasedConnection() throws Exception {
        DBPool pool = newPool(1);
        Connection held = pool.getConnection();
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Thread> waiterThread = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Connection> waitingBorrower = executor.submit(() -> {
                waiterThread.set(Thread.currentThread());
                started.countDown();
                return pool.getConnection();
            });
            assertTrue(started.await(2, TimeUnit.SECONDS));
            awaitWaiting(waiterThread.get());

            pool.releaseConnection(held);

            assertSame(held, waitingBorrower.get(2, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    /**
     * Concurrent borrowers never overlap on one physical connection or exceed
     * capacity.
     */
    @Test
    void concurrentBorrowingRespectsOwnershipAndCapacity() throws Exception {
        int maximum = 4;
        int workers = maximum;
        DBPool pool = newPool(maximum);
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch firstWaveBorrowed = new CountDownLatch(maximum);
        CountDownLatch releaseFirstWave = new CountDownLatch(1);
        AtomicInteger borrowCount = new AtomicInteger();
        ConcurrentHashMap<Connection, Thread> activeConnections = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (int worker = 0; worker < workers; worker++) {
                tasks.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    for (int iteration = 0; iteration < 40; iteration++) {
                        Connection connection = pool.getConnection();
                        Thread currentThread = Thread.currentThread();
                        Thread previousOwner = activeConnections.putIfAbsent(connection, currentThread);
                        try {
                            if (previousOwner != null) {
                                throw new AssertionError("Connection is borrowed by two threads");
                            }
                            if (borrowCount.incrementAndGet() <= maximum) {
                                firstWaveBorrowed.countDown();
                                if (!releaseFirstWave.await(2, TimeUnit.SECONDS)) {
                                    throw new AssertionError("First wave was not released");
                                }
                            }
                        } finally {
                            activeConnections.remove(connection, currentThread);
                            pool.releaseConnection(connection);
                        }
                    }
                    return null;
                }));
            }
            assertTrue(ready.await(2, TimeUnit.SECONDS));
            start.countDown();
            assertTrue(firstWaveBorrowed.await(2, TimeUnit.SECONDS));
            assertEquals(maximum, activeConnections.size());
            assertEquals(maximum, driver.connections.size());
            releaseFirstWave.countDown();

            for (Future<?> task : tasks) {
                task.get(5, TimeUnit.SECONDS);
            }
            assertTrue(driver.connections.size() <= maximum);
        } finally {
            releaseFirstWave.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    /** Releasing the same connection twice does not make it available twice. */
    @Test
    void doubleReleaseDoesNotDuplicateIdleConnection() throws Exception {
        DBPool pool = newPool(1);
        Connection connection = pool.getConnection();
        pool.releaseConnection(connection);
        pool.releaseConnection(connection);

        assertSame(connection, pool.getConnection());
        assertThrows(SQLException.class, pool::getConnection);
        assertEquals(1, driver.connections.size());
    }

    /** Releasing a connection not owned by this pool leaves the pool unchanged. */
    @Test
    void foreignConnectionDoesNotCorruptPool() throws Exception {
        DBPool pool = newPool(1);
        Connection owned = pool.getConnection();
        Connection foreign = driver.connect(driver.url, new Properties());

        pool.releaseConnection(foreign);
        pool.releaseConnection(owned);

        assertSame(owned, pool.getConnection());
        assertFalse(foreign.isClosed());
        foreign.close();
    }

    /** Shutdown closes connections that are idle at shutdown time. */
    @Test
    void shutdownClosesIdleConnections() throws Exception {
        DBPool pool = newPool(1);
        Connection connection = pool.getConnection();
        pool.releaseConnection(connection);

        pool.shutdown();

        assertTrue(connection.isClosed());
        assertThrows(SQLException.class, pool::getConnection);
    }

    /**
     * Shutdown rejects new acquisitions and closes a borrowed connection on return.
     */
    @Test
    void shutdownClosesBorrowedConnectionWhenReturned() throws Exception {
        DBPool pool = newPool(1);
        Connection borrowed = pool.getConnection();

        pool.shutdown();

        assertThrows(SQLException.class, pool::getConnection);
        assertFalse(borrowed.isClosed());
        pool.releaseConnection(borrowed);
        assertTrue(borrowed.isClosed());
    }

    /** An already terminated pool reports termination immediately. */
    @Test
    void awaitTerminationReturnsTrueWhenAlreadyTerminated() throws Exception {
        DBPool pool = newPool(1);
        pool.shutdown();
        long start = System.nanoTime();

        assertTrue(pool.awaitTermination(2, TimeUnit.SECONDS));

        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 500,
                "An already terminated pool should return immediately");
    }

    /** A borrowed connection keeps a shut down pool alive until the timeout. */
    @Test
    void awaitTerminationTimesOutWhileConnectionIsBorrowed() throws Exception {
        DBPool pool = newPool(1);
        Connection borrowed = pool.getConnection();
        pool.shutdown();
        long start = System.nanoTime();

        try {
            assertFalse(pool.awaitTermination(200, TimeUnit.MILLISECONDS));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMillis >= 150, "Timed out too early: " + elapsedMillis + " ms");
            assertTrue(elapsedMillis < 1500, "Timed out too late: " + elapsedMillis + " ms");
        } finally {
            pool.releaseConnection(borrowed);
        }
    }

    /** Releasing the last borrowed connection wakes a termination waiter. */
    @Test
    void awaitTerminationCompletesAfterBorrowedConnectionIsReleased() throws Exception {
        DBPool pool = newPool(1);
        Connection borrowed = pool.getConnection();
        pool.shutdown();
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Thread> waiterThread = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        boolean released = false;
        try {
            Future<Boolean> termination = executor.submit(() -> {
                waiterThread.set(Thread.currentThread());
                started.countDown();
                return pool.awaitTermination(5, TimeUnit.SECONDS);
            });
            assertTrue(started.await(2, TimeUnit.SECONDS));
            awaitWaiting(waiterThread.get());

            pool.releaseConnection(borrowed);
            released = true;

            assertTrue(termination.get(2, TimeUnit.SECONDS));
        } finally {
            if (!released) {
                pool.releaseConnection(borrowed);
            }
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    /**
     * A zero timeout returns false immediately while a borrowed connection remains.
     */
    @Test
    void awaitTerminationWithZeroTimeoutDoesNotWait() throws Exception {
        DBPool pool = newPool(1);
        Connection borrowed = pool.getConnection();
        pool.shutdown();
        long start = System.nanoTime();

        try {
            assertFalse(pool.awaitTermination(0, TimeUnit.MILLISECONDS));
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 500,
                    "A zero timeout should return without waiting");
        } finally {
            pool.releaseConnection(borrowed);
        }
    }

    /**
     * A waiting termination call exits with InterruptedException when interrupted.
     */
    @Test
    void awaitTerminationRespondsToInterruption() throws Exception {
        DBPool pool = newPool(1);
        Connection borrowed = pool.getConnection();
        pool.shutdown();
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            started.countDown();
            try {
                outcome.set(pool.awaitTermination(5, TimeUnit.SECONDS)
                        ? new AssertionError("Pool terminated before the borrowed connection was released")
                        : new AssertionError("Termination wait unexpectedly timed out"));
            } catch (InterruptedException interrupted) {
                outcome.set(interrupted);
            }
        });
        waiter.start();

        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            awaitWaiting(waiter);
            waiter.interrupt();
            waiter.join(2_000);

            assertFalse(waiter.isAlive(), "Interrupted termination waiter did not exit");
            assertInstanceOf(InterruptedException.class, outcome.get());
        } finally {
            if (waiter.isAlive()) {
                waiter.interrupt();
                waiter.join(2_000);
            }
            pool.releaseConnection(borrowed);
        }
    }

    /** A creation that finishes after shutdown is closed and never handed out. */
    @Test
    void shutdownDuringCreationClosesNewConnection() throws Exception {
        DBPool pool = newPool(1);
        driver.connectEntered = new CountDownLatch(1);
        driver.allowConnect = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Connection> creation = executor.submit(pool::getConnection);
            assertTrue(driver.connectEntered.await(2, TimeUnit.SECONDS));

            pool.shutdown();
            driver.allowConnect.countDown();

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> creation.get(2, TimeUnit.SECONDS));
            assertInstanceOf(SQLException.class, failure.getCause());
            assertEquals(1, driver.connections.size());
            assertTrue(driver.connections.get(0).isClosed());
        } finally {
            driver.allowConnect.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    /**
     * Failed physical creation rolls back its reservation so a later attempt can
     * succeed.
     */
    @Test
    void failedCreationDoesNotCorruptPoolAccounting() throws Exception {
        DBPool pool = newPool(1);
        driver.failuresRemaining.set(1);

        assertThrows(SQLException.class, pool::getConnection);
        Connection recovered = pool.getConnection();

        assertSame(recovered, driver.connections.get(0));
        assertEquals(2, driver.attempts.get());
        assertEquals(1, driver.connections.size());
    }

    /**
     * An interrupted waiter exits with InterruptedException and leaves the pool
     * usable.
     */
    @Test
    void interruptedWaiterExitsPromptly() throws Exception {
        DBPool pool = newPool(1);
        Connection held = pool.getConnection();
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            started.countDown();
            try {
                pool.getConnection();
            } catch (Throwable failure) {
                outcome.set(failure);
            }
        });
        waiter.start();

        assertTrue(started.await(2, TimeUnit.SECONDS));
        awaitWaiting(waiter);
        waiter.interrupt();
        waiter.join(2_000);

        assertFalse(waiter.isAlive(), "Interrupted waiter did not exit");
        assertInstanceOf(InterruptedException.class, outcome.get());
        pool.releaseConnection(held);
        assertSame(held, pool.getConnection());
    }

    private DBPool newPool(int maximum) {
        DBPool pool = new DBPool(maximum, driver.url, "user", "password");
        pools.add(pool);
        return pool;
    }

    private static void awaitWaiting(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING
                && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertTrue(thread.getState() == Thread.State.WAITING
                || thread.getState() == Thread.State.TIMED_WAITING,
                "Thread did not enter the pool wait: " + thread.getState());
    }

    private static final class TestDriver implements Driver {
        private final String url = "jdbc:dbpool-test:" + System.nanoTime();
        private final List<Connection> connections = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final AtomicInteger attempts = new AtomicInteger();
        private final AtomicInteger failuresRemaining = new AtomicInteger();
        private volatile CountDownLatch connectEntered;
        private volatile CountDownLatch allowConnect;

        @Override
        public Connection connect(String requestedUrl, Properties properties) throws SQLException {
            if (!acceptsURL(requestedUrl)) {
                return null;
            }
            attempts.incrementAndGet();
            if (failuresRemaining.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
                throw new SQLException("Configured test connection failure");
            }
            if (connectEntered != null) {
                connectEntered.countDown();
                try {
                    if (!allowConnect.await(2, TimeUnit.SECONDS)) {
                        throw new SQLException("Timed out waiting to release test connection creation");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while creating test connection", interrupted);
                }
            }
            AtomicReference<Boolean> closed = new AtomicReference<>(false);
            Connection connection = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(), new Class<?>[] { Connection.class },
                    (proxy, method, arguments) -> {
                        switch (method.getName()) {
                            case "close":
                                closed.set(true);
                                return null;
                            case "isClosed":
                                return closed.get();
                            case "equals":
                                return proxy == arguments[0];
                            case "hashCode":
                                return System.identityHashCode(proxy);
                            case "toString":
                                return "TestConnection@" + System.identityHashCode(proxy);
                            default:
                                return defaultValue(method.getReturnType());
                        }
                    });
            connections.add(connection);
            return connection;
        }

        @Override
        public boolean acceptsURL(String requestedUrl) {
            return url.equals(requestedUrl);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String requestedUrl, Properties properties) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        private static Object defaultValue(Class<?> returnType) {
            if (!returnType.isPrimitive()) {
                return null;
            }
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == char.class) {
                return '\0';
            }
            if (returnType == byte.class) {
                return (byte) 0;
            }
            if (returnType == short.class) {
                return (short) 0;
            }
            if (returnType == int.class) {
                return 0;
            }
            if (returnType == long.class) {
                return 0L;
            }
            if (returnType == float.class) {
                return 0F;
            }
            return 0D;
        }
    }
}