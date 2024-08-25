package io.youngwon.bufferable;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BufferableImplTest {
    private static RedisServer redisServer;
    private static BufferableImpl bufferable;

    @BeforeAll
    public static void setUpAll() throws IOException {
        int port = 63791;
        redisServer = new RedisServer(port);
        redisServer.start();
        bufferable = new BufferableImpl(
                "test",

                3,
                -1,
                "localhost",
                port
        );
    }

    @AfterAll
    public static void tearDownAll() throws IOException {
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @BeforeEach
    public void setUp() {

    }

    @Test
    public void isProceed_test() throws ExecutionException, InterruptedException {
        IsProceedAndRank result = bufferable.isProceed("token01");
        assertTrue(result.getIsProceed());
        assertEquals(-1, result.getRank());
        assertEquals(0, bufferable.getWaitSize());

        result = bufferable.isProceed("token02");
        assertTrue(result.getIsProceed());
        assertEquals(-1, result.getRank());
        assertEquals(0, bufferable.getWaitSize());

        result = bufferable.isProceed("token03");
        assertTrue(result.getIsProceed());
        assertEquals(-1, result.getRank());
        assertEquals(0, bufferable.getWaitSize());

        result = bufferable.isProceed("token04");
        assertFalse(result.getIsProceed());
        assertEquals(0, result.getRank());
        assertEquals(1, bufferable.getWaitSize());

        result = bufferable.isProceed("token02");
        assertTrue(result.getIsProceed());
        assertEquals(-1, result.getRank());

        result = bufferable.isProceed("token05");
        assertFalse(result.getIsProceed());
        assertEquals(1, result.getRank());
        assertEquals(2, bufferable.getWaitSize());

        assertTrue(bufferable.isDone("token01"));

        assertEquals(1, bufferable.moveWaitToProceed());

        result = bufferable.isProceed("token04");
        assertTrue(result.getIsProceed());
        assertEquals(-1, result.getRank());

        result = bufferable.isProceed("token05");
        assertFalse(result.getIsProceed());
        assertEquals(0, result.getRank());
        assertEquals(1, bufferable.getWaitSize());
    }
}
