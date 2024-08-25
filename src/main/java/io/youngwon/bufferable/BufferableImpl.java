package io.youngwon.bufferable;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;

public class BufferableImpl implements Bufferable, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferableImpl.class);

    private final String name;
    private final Integer port;
    private final Long maxProceedSize;
    private final Long intervalWaitToProceed;
    private final String[] keys;


    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // TODO from configuration
    private final RedisClient redisClient;

    // TODO close
    private final StatefulRedisConnection<String, String> connection;
    private final RedisAsyncCommands<String, String> commands;

    private String tokenScript = """
            local n = tonumber(ARGV[1])
            local token = ARGV[2]
            local currentTimeMillis = tonumber(ARGV[3])
                        
            local proceed_key = KEYS[1]
            local wait_key = KEYS[2]
                        
            if redis.call('sismember', proceed_key, token) == 1 then
                return {1, -1}
            end

            local rank = redis.call('zrank', wait_key, token)
            if rank then
                return {0, rank}
            end

            if redis.call('zcard', wait_key) == 0 and redis.call('scard', proceed_key) < n then
                redis.call('sadd', proceed_key, token)
                return {1, -1}
            end

            redis.call('zadd', wait_key, currentTimeMillis, token)
            rank = redis.call('zrank', wait_key, token)
            return {0, rank}
            """;

    private String moveScript = """
            local proceed_size = redis.call('scard', KEYS[1])
            local max_proceed_size = tonumber(ARGV[1])
            local move_count = max_proceed_size - proceed_size

            if move_count > 0 then
                local items_to_move = redis.call('zrange', KEYS[2], 0, move_count - 1)

                if #items_to_move > 0 then
                    for _, item in ipairs(items_to_move) do
                        local score = redis.call('zscore', KEYS[2], item)
                        redis.call('sadd', KEYS[1], item)
                        redis.call('zrem', KEYS[2], item)
                    end
                    return #items_to_move
                end
            end

            return 0
            """;

    public BufferableImpl(
            String name,
            long maxProceedSize,
            long intervalWaitToProceed,
            String host,
            int port) {

        this.name = name;
        this.port = port;
        this.redisClient = RedisClient.create("redis://" + host + ":" + port);
        this.maxProceedSize = maxProceedSize;
        this.intervalWaitToProceed = intervalWaitToProceed;
        this.connection = redisClient.connect();
        this.commands = connection.async();
        this.keys = new String[]{
                "bufferable:%s:proceed".formatted(name),
                "bufferable:%s:wait".formatted(name)
        };
        if (intervalWaitToProceed > 0) {
            this.start();
        }

    }

    @Override
    public void close() {
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        connection.close();
        redisClient.close();
        LOGGER.info("{} bufferable is close", name);
    }

    @Override
    public IsProceedAndRank isProceed(String token) {
        CompletableFuture<IsProceedAndRank> future = commands.eval(tokenScript, ScriptOutputType.MULTI, keys,
                        maxProceedSize.toString(),
                        token,
                        String.valueOf(currentTimeMillis()))
                .toCompletableFuture()
                .thenApply(result -> {
                    if (result instanceof List) {
                        List<Object> resultList = (List<Object>) result;
                        Boolean isProceed = ((Number) resultList.get(0)).longValue() == 1;
                        Long rank = ((Number) resultList.get(1)).longValue();
                        LOGGER.info("complete token {}, {}, rank : {}", token, isProceed, rank);
                        return IsProceedAndRank.of(isProceed, rank);
                    } else {
                        LOGGER.error("fail to check type");
                    }
                    return IsProceedAndRank.ofFail();
                }).exceptionally(throwable -> {
                    LOGGER.error("fail to cast", throwable);
                    return IsProceedAndRank.ofFail();
                });

        try {
            return future.get();
        } catch (Exception e) {
            LOGGER.error("{} bufferable fail to execute isProceed", name, e);
        }
        return null;
    }


    @Override
    public Boolean isDone(String token) {
        try {
            return commands.srem(keys[0], token).get() > 0;
        } catch (Exception e) {
            LOGGER.error("{} bufferable fail to remove token {}", name, token, e);
        }
        return false;
    }

    @Override
    public Long getWaitSize() {
        try {
            return commands.zcard(keys[1]).get();
        } catch (Exception e) {
            LOGGER.error("{} bufferable fail to zcard ", name, e);
        }
        return 0L;
    }

    private void start() {
        scheduler.scheduleAtFixedRate(this::moveWaitToProceed, 0, intervalWaitToProceed, TimeUnit.MILLISECONDS);
        LOGGER.info("{} bufferable start bufferablem", name);
    }

    public Long moveWaitToProceed() {
        // TODO using (lock)
        try {
            return commands.eval(moveScript, ScriptOutputType.INTEGER, keys, maxProceedSize.toString())
                    .toCompletableFuture()
                    .thenApply(moveCount -> {
                        LOGGER.info("{} bufferable complete to move {} tokens from wait queue to proceed queue", name, moveCount);
                        return (Long) moveCount;
                    }).get();
        } catch (Exception e) {
            LOGGER.error("{} bufferable fail to move token", name, e);
        }

        return 0L;
    }
}
