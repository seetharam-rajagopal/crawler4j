package edu.uci.ics.crawler4j.redis;

import java.util.Arrays;
import java.util.List;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

public class CustomDatabase {

    public enum DB {
        docIDsDB(1),
        statisticsDB(2),
        urlsDB(3);

        private final int value;

        DB(int value) {
            this.value = value;
        }
    }

    private static final RedisCommands<byte[], byte[]> syncCommands;

    static {
        RedisClient redisClient = RedisClient.create("redis://localhost:6379/");
        StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
        syncCommands = connection.sync();
    }

    public static String set(DB databaseName, byte[] key, byte[] value) {
        syncCommands.select(databaseName.value);
        return syncCommands.set(key, value);
    }

    public static String set(DB databaseName, String key, String value) {
        syncCommands.select(databaseName.value);
        return syncCommands.set(key.getBytes(), value.getBytes());
    }

    public static byte[] get(DB databaseName, byte[] key) {
        syncCommands.select(databaseName.value);
        return syncCommands.get(key);
    }

    public static String get(DB databaseName, String key) {
        syncCommands.select(databaseName.value);
        return Arrays.toString(syncCommands.get(key.getBytes()));
    }

    public static long count(DB databaseName) {
        syncCommands.select(databaseName.value);
        return syncCommands.dbsize();
    }

    public static void flushDbList(List<DB> databaseNames) {
        databaseNames.forEach(databaseName -> {
            syncCommands.select(databaseName.value);
            syncCommands.flushdb();
        });
    }
}
