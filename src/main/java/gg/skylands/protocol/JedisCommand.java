package gg.skylands.protocol;

import redis.clients.jedis.Jedis;

@FunctionalInterface
public interface JedisCommand<T> {

    /**
     * Run a command on a Jedis instance.
     * @param jedis The Jedis instance.
     * @return The result of the command.
     */
    T run(Jedis jedis);

}
