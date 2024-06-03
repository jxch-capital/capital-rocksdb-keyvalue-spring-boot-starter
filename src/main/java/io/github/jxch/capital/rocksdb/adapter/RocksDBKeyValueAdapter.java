package io.github.jxch.capital.rocksdb.adapter;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.util.CloseableIterator;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.springframework.util.SerializationUtils.deserialize;
import static org.springframework.util.SerializationUtils.serialize;

@Slf4j
@RequiredArgsConstructor
public class RocksDBKeyValueAdapter implements KeyValueAdapter {
    private final RocksDB rocksDB;

    private byte[] createKey(Object id, String keyspace) {
        return (keyspace + ":" + id).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    @SneakyThrows
    public Object put(Object id, Object item, String keyspace) {
        rocksDB.put(createKey(id, keyspace), serialize(item));
        return item;
    }

    @Override
    @SneakyThrows
    public boolean contains(Object id, String keyspace) {
        return Objects.nonNull(rocksDB.get(createKey(id, keyspace)));
    }

    @Override
    @SneakyThrows
    public Object get(Object id, String keyspace) {
        return deserialize(rocksDB.get(createKey(id, keyspace)));
    }

    @Override
    public <T> T get(Object id, String keyspace, Class<T> type) {
        return type.cast(get(id, keyspace));
    }

    @Override
    @SneakyThrows
    public Object delete(Object id, String keyspace) {
        byte[] key = createKey(id, keyspace);
        byte[] value = rocksDB.get(key);
        if (Objects.nonNull(value)) {
            rocksDB.delete(key);
            return deserialize(value);
        }
        return null;
    }

    @Override
    public <T> T delete(Object id, String keyspace, Class<T> type) {
        return Optional.of(delete(id, keyspace)).map(type::cast).orElse(null);
    }

    private final static Runnable WARN_UNSUPPORTED_LOG = () -> log.warn("遍历所有元素，效率不高，不建议使用");

    @Override
    @SneakyThrows
    public void deleteAllOf(String keyspace) {
        WARN_UNSUPPORTED_LOG.run();
        try (final RocksIterator iterator = rocksDB.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (key.startsWith(keyspace + ":")) {
                    rocksDB.delete(iterator.key());
                }
            }
        }
    }

    @Override
    public Iterable<?> getAllOf(String keyspace) {
        WARN_UNSUPPORTED_LOG.run();
        List<Object> results = new ArrayList<>();
        try (final RocksIterator iterator = rocksDB.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (key.startsWith(keyspace + ":")) {
                    Object value = deserialize(iterator.value());
                    results.add(value);
                }
            }
        }
        return results;
    }

    @Override
    public CloseableIterator<Map.Entry<Object, Object>> entries(String keyspace) {
        WARN_UNSUPPORTED_LOG.run();
        final RocksIterator iterator = rocksDB.newIterator();

        return new CloseableIterator<Map.Entry<Object, Object>>() {
            private Map.Entry<Object, Object> nextEntry;

            // Initialize and position the iterator at the first relevant entry
            {
                seekToFirstRelevant();
            }

            private void seekToFirstRelevant() {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    if (new String(iterator.key(), StandardCharsets.UTF_8).startsWith(keyspace + ":")) {
                        nextEntry = new AbstractMap.SimpleEntry<>(
                                deserialize(iterator.key()),
                                deserialize(iterator.value()));
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return nextEntry != null;
            }

            @Override
            public Map.Entry<Object, Object> next() {
                if (!hasNext()) throw new NoSuchElementException();
                Map.Entry<Object, Object> currentEntry = nextEntry;
                fetchNext();
                return currentEntry;
            }

            private void fetchNext() {
                nextEntry = null;
                for (iterator.next(); iterator.isValid(); iterator.next()) {
                    if (new String(iterator.key(), StandardCharsets.UTF_8).startsWith(keyspace + ":")) {
                        nextEntry = new AbstractMap.SimpleEntry<>(
                                deserialize(iterator.key()),
                                deserialize(iterator.value()));
                        return;
                    }
                }
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

    @Override
    public long count(String keyspace) {
        WARN_UNSUPPORTED_LOG.run();
        long count = 0;
        try (final RocksIterator iterator = rocksDB.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                if (new String(iterator.key(), StandardCharsets.UTF_8).startsWith(keyspace + ":")) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public long count(KeyValueQuery<?> query, String keyspace) {
        WARN_UNSUPPORTED_LOG.run();
        RocksdbQueryCriteria criteria = (RocksdbQueryCriteria) query.getCriteria();
        long count = 0;
        try (final RocksIterator iterator = rocksDB.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (criteria.matchesKey(key) && key.startsWith(keyspace)) {
                    String value = new String(iterator.value(), StandardCharsets.UTF_8);
                    if (criteria.matchesValue(value)) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    @Override
    public <T> Iterable<T> find(KeyValueQuery<?> query, String keyspace, Class<T> type) {
        WARN_UNSUPPORTED_LOG.run();
        RocksdbQueryCriteria criteria = (RocksdbQueryCriteria) query.getCriteria();
        List<T> results = new ArrayList<>();
        try (final RocksIterator iterator = rocksDB.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (criteria.matchesKey(key)) {
                    T value = type.cast(deserialize(iterator.value()));
                    String valueAsString = value.toString();
                    if (criteria.matchesValue(valueAsString)) {
                        results.add(value);
                    }
                }
            }
        }
        return results;
    }

    @Override
    @SneakyThrows
    public void clear() {
        rocksDB.close();
    }

    @Override
    public void destroy() {
        rocksDB.close();
    }

}
