package io.github.jxch.capital.rocksdb.test;

import io.github.jxch.capital.rocksdb.RocksdbTemplate;
import io.github.jxch.capital.rocksdb.config.RocksDBAutoConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest(classes = RocksDBAutoConfig.class)
class EntityRepositoryTest {
    @Autowired
    private RocksdbTemplate template;

    @Test
    public void test() {
        template.insert(EntityTest.builder()
                        .id("12345")
                        .name("test5")
                        .description("the test6")
                .build());
        Iterable<EntityTest> entityTests = template.findAll(EntityTest.class);

    }

}