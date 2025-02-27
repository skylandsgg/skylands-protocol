package gg.skylands.protocol;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SkylandsRedis {

    public static String PREFIX = "Skylands.";

    private static SkylandsRedis instance;

    private final ExecutorService threadPool;

    private final JedisPool pool;

    /**
     * Connect to Redis using the credentials from the REDIS_CREDS environment variable.
     * @throws IllegalStateException If the instance is already initialized, or if the REDIS_CREDS environment variable is not set or if the credentials are invalid.
     */
    public SkylandsRedis() throws IllegalStateException {
        if (instance != null) throw new IllegalStateException("SkylandsRedis instance is already initialized!");

        instance = this;

        String creds = System.getenv("REDIS_CREDS");
        if (creds == null || creds.isBlank()) throw new IllegalStateException("REDIS_CREDS environment variable is not set!");

        String[] split = creds.split(":");
        boolean hasUser = split.length == 4;

        String host = split[0];
        int port = Integer.parseInt(split[1]);

        String user = hasUser ? split[2] : null;
        String password = hasUser ? split[3] : split[2];

        this.threadPool = Executors.newCachedThreadPool();

        try {
            this.pool = new JedisPool(host, port, (user == null || user.isBlank() ? null : user), password);

            // Send a ping to the Redis server
            long start = System.currentTimeMillis();
            runCommand(jedis -> {
                System.out.printf("[skylands-protocol] Redis ping: %s (%s ms)%n", jedis.ping(), System.currentTimeMillis() - start);
                return null;
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize SkylandsRedis!", e);
        }
    }

    /**
     * Connect to Redis using the given credentials.
     * @param host Host of the Redis server
     * @param port Port of the Redis server
     * @param user User of the Redis server
     * @param password Password of the Redis server
     * @throws IllegalStateException If the instance is already initialized, or if the credentials are invalid.
     */
    public SkylandsRedis(@NotNull String host, int port, @Nullable String user, @NotNull String password) throws IllegalStateException {
        if (instance != null) throw new IllegalStateException("SkylandsRedis instance is already initialized!");

        instance = this;

        this.threadPool = Executors.newCachedThreadPool();

        try {
            this.pool = new JedisPool(host, port, (user == null || user.isBlank() ? null : user), password);

            // Send a ping to the Redis server
            long start = System.currentTimeMillis();
            runCommand(jedis -> {
                System.out.printf("[skylands-protocol] Redis ping: %s (%s ms)%n", jedis.ping(), System.currentTimeMillis() - start);
                return null;
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize SkylandsRedis!", e);
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
     * Disconnect from the Redis database.
     */
    public void close() {
        pool.close();
    }
}
