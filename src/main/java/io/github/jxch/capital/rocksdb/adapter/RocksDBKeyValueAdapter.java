package io.github.jxch.capital.rocksdb.adapter;

import com.alibaba.fastjson2.JSON;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.rocksdb.RocksDB;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.util.CloseableIterator;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.*;

@RequiredArgsConstructor
public class RocksDBKeyValueAdapter implements KeyValueAdapter {
    private final RocksDB rocksDB;

    @NonNull
    private byte[] createKey(@NonNull Object id, @NonNull String keyspace) {
        if (id instanceof String) {
            return (keyspace + ":" + id).getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException("id must be a string");
    }

    @SneakyThrows
    private byte[] getKeyspace(@NonNull String keyspace) {
        return rocksDB.get(keyspace.getBytes(StandardCharsets.UTF_8));
    }

    private boolean containsKeyspace(@NonNull String keyspace) {
        return Objects.nonNull(getKeyspace(keyspace));
    }

    private Set<String> getKeyspaceIds(String keyspace) {
        return containsKeyspace(keyspace) ? new HashSet<>(JSON.parseArray(getKeyspace(keyspace), String.class)) : Set.of();
    }

    @SneakyThrows
    private void putKeyspaceIds(@NonNull String keyspace, @NonNull Set<String> ids) {
        rocksDB.put(keyspace.getBytes(StandardCharsets.UTF_8), JSON.toJSONString(ids).getBytes(StandardCharsets.UTF_8));
    }

    private void putKeyspaceId(@NonNull String keyspace, @NonNull Object id) {
        Set<String> ids = new HashSet<>(getKeyspaceIds(keyspace));
        ids.add(id.toString());
        putKeyspaceIds(keyspace, ids);
    }

    private void deleteKeyspaceId(@NonNull String keyspace, @NonNull Object id) {
        Set<String> ids = new HashSet<>(getKeyspaceIds(keyspace));
        ids.remove(id.toString());
        if (ids.isEmpty()) {
            deleteKeyspace(keyspace);
        } else {
            putKeyspaceIds(keyspace, ids);
        }
    }

    @SneakyThrows
    private void deleteKeyspace(@NonNull String keyspace) {
        rocksDB.delete(keyspace.getBytes(StandardCharsets.UTF_8));
    }

    @NonNull
    @Override
    @SneakyThrows
    @Transactional
    public Object put(@NonNull Object id, @NonNull Object item, @NonNull String keyspace) {
        putKeyspaceId(keyspace, id);
        rocksDB.put(createKey(id, keyspace), JSON.toJSONString(item).getBytes(StandardCharsets.UTF_8));
        return item;
    }

    @Override
    public boolean contains(@NonNull Object id, @NonNull String keyspace) {
        return getKeyspaceIds(keyspace).contains(id.toString());
    }

    @Nullable
    @Override
    @SneakyThrows
    public Object get(@NonNull Object id, @NonNull String keyspace) {
        return contains(id, keyspace) ? JSON.parse(rocksDB.get(createKey(id, keyspace))) : null;
    }

    @Nullable
    @Override
    public <T> T get(@NonNull Object id, @NonNull String keyspace, @NonNull Class<T> type) {
        return Optional.ofNullable(get(id, keyspace)).map(type::cast).orElse(null);
    }

    @Nullable
    @Override
    @SneakyThrows
    @Transactional
    public Object delete(@NonNull Object id, @NonNull String keyspace) {
        Object value = get(id, keyspace);
        if (Objects.nonNull(value)) {
            rocksDB.delete(createKey(id, keyspace));
            deleteKeyspaceId(keyspace, id);
            return value;
        }
        return null;
    }

    @Nullable
    @Override
    @Transactional
    public <T> T delete(@NonNull Object id, @NonNull String keyspace, @NonNull Class<T> type) {
        deleteKeyspaceId(keyspace, id);
        return Optional.ofNullable(delete(id, keyspace)).map(type::cast).orElse(null);
    }

    @Override
    @SneakyThrows
    @Transactional
    public void deleteAllOf(@NonNull String keyspace) {
        getKeyspaceIds(keyspace).forEach(id -> delete(id, keyspace));
        deleteKeyspace(keyspace);
    }

    @NonNull
    @Override
    public Iterable<?> getAllOf(@NonNull String keyspace) {
        return getKeyspaceIds(keyspace).stream().map(id -> get(id, keyspace)).toList();
    }

    @Override
    public long count(@NonNull String keyspace) {
        return getKeyspaceIds(keyspace).size();
    }

    @Override
    public long count(@NonNull KeyValueQuery<?> query, @NonNull String keyspace) {
        RocksdbQueryCriteria criteria = (RocksdbQueryCriteria) query.getCriteria();
        return getKeyspaceIds(keyspace).stream().filter(id ->
                Objects.isNull(criteria) || criteria.matches(keyspace, id, get(id, keyspace))).count();
    }

    @NonNull
    @Override
    public <T> Iterable<T> find(@NonNull KeyValueQuery<?> query, @NonNull String keyspace, @NonNull Class<T> type) {
        RocksdbQueryCriteria criteria = (RocksdbQueryCriteria) query.getCriteria();
        return getKeyspaceIds(keyspace).stream().map(id -> {
            Object value = get(id, keyspace);
            return (Objects.isNull(criteria) || criteria.matches(keyspace, id, value)) ? type.cast(value) : null;
        }).filter(Objects::nonNull).toList();
    }

    @NonNull
    @Override
    public CloseableIterator<Map.Entry<Object, Object>> entries(@NonNull String keyspace) {
        return new CloseableIterator<>() {
            private final Iterator<String> iterator = getKeyspaceIds(keyspace).iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Map.Entry<Object, Object> next() {
                String id = iterator.next();
                return new AbstractMap.SimpleEntry<>(createKey(id, keyspace), get(id, keyspace));
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public void clear() {
        rocksDB.close();
    }

    @Override
    public void destroy() {
        rocksDB.close();
    }
}
