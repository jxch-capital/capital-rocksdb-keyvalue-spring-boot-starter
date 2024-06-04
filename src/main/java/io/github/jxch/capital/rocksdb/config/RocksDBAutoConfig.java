package io.github.jxch.capital.rocksdb.config;

import io.github.jxch.capital.rocksdb.RocksdbTemplate;
import io.github.jxch.capital.rocksdb.adapter.RocksDBKeyValueAdapter;
import lombok.Data;
import lombok.SneakyThrows;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.core.KeyValueTemplate;

@Data
@Configuration
public class RocksDBAutoConfig {
    public static final String ROCKSDB_TEMPLATE = "rocksdbKeyValueTemplate";
    @Value("${spring.rocksdb.path:rocksdb}")
    private String path;

    @Bean
    @SneakyThrows
    @ConditionalOnMissingBean(RocksDB.class)
    public RocksDB rocksDB() {
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        return RocksDB.open(options, path);
    }

    @Bean(ROCKSDB_TEMPLATE)
    @ConditionalOnMissingBean(name = ROCKSDB_TEMPLATE)
    public KeyValueOperations rocksdbKeyValueTemplate(RocksDBKeyValueAdapter adapter) {
        return new KeyValueTemplate(adapter);
    }

    @Bean
    @ConditionalOnMissingBean(RocksdbTemplate.class)
    public RocksdbTemplate rocksdbTemplate(RocksDBKeyValueAdapter adapter) {
        return new RocksdbTemplate(adapter);
    }

    @Bean
    @ConditionalOnMissingBean(RocksDBKeyValueAdapter.class)
    public RocksDBKeyValueAdapter rocksdbKeyValueAdapter(RocksDB rocksDB) {
        return new RocksDBKeyValueAdapter(rocksDB);
    }
}
