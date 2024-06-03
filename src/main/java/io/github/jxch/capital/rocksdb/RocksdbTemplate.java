package io.github.jxch.capital.rocksdb;

import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueTemplate;

public class RocksdbTemplate extends KeyValueTemplate {

    public RocksdbTemplate(KeyValueAdapter adapter) {
        super(adapter);
    }

}
