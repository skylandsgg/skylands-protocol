package gg.skylands.protocol;

import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SkylandsRedis {

    public static String PREFIX = "Skylands.";

    private static SkylandsRedis instance;
    private static boolean state = false;

    private final ExecutorService threadPool;

    private JedisPool pool;

    public SkylandsRedis(@NotNull String host, int port, @NotNull String password) {
        if (instance != null) throw new IllegalStateException("SkylandsRedis instance is already initialized!");

        instance = this;

        this.threadPool = Executors.newCachedThreadPool();

        try {

            this.pool = new JedisPool(host, port, null, password);

            // Send a ping to the Redis server
            long start = System.currentTimeMillis();
            runCommand(jedis -> {
                System.out.printf("Redis ping: %s (%s ms)%n", jedis.ping(), System.currentTimeMillis() - start);
                return null;
            });

        } catch (Exception e) {
            state = false;

            System.err.println("Could not connect to Redis database! Some features may not work.");
        }
    }

    /**
     * Run a command on the Redis server.
     * @param command The command to run.
     * @return The result of the command.
     * @param <T> The type of the result.
     */
    public <T> T runCommand(@NotNull JedisCommand<T> command) {
        if (this.pool.isClosed()) return null;

        try (Jedis jedis = this.pool.getResource()) {
            return command.run(jedis);
        }
    }

    /**
     * Run an asynchronous command on the Redis server.
     * @param command The command to run.
     * @return A CompletableFuture of the result of the command.
     * @param <T> The type of the result.
     */
    public <T> CompletableFuture<T> awaitCommand(@NotNull JedisCommand<T> command) {
        return CompletableFuture.supplyAsync(() -> runCommand(command), threadPool);
    }

    @NotNull
    public static SkylandsRedis instance() {
        if (instance == null) throw new IllegalStateException("SkylandsRedis instance is not initialized yet!");

        return instance;
    }

    /**
     * Returns true if Redis is enabled, false otherwise.
     * @return  true if Redis is enabled, false otherwise.
     */
    private boolean checkState() {
        return state;
    }

    /**
     * Disconnect from the Redis database.
     */
    public void close() {
        if (!checkState()) return;

        pool.close();
    }
}
