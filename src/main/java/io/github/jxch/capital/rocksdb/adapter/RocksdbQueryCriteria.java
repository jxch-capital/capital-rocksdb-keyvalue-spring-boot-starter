package io.github.jxch.capital.rocksdb.adapter;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class RocksdbQueryCriteria {
    private final String keyPrefix;
    private final String valueContains;

    public boolean matchesKey(String key) {
        return key.startsWith(this.keyPrefix);
    }

    public boolean matchesValue(String value) {
        return value.contains(this.valueContains);
    }

}
