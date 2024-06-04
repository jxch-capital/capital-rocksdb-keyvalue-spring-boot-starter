# capital-rocksdb-keyvalue-spring-boot-starter
RocksDB 集成 Spring Data KeyValue

## 示例

```java
import io.github.jxch.capital.rocksdb.config.RocksDBAutoConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.map.repository.config.EnableMapRepositories;

@Configuration
@EnableMapRepositories(keyValueTemplateRef = RocksDBAutoConfig.ROCKSDB_KV_TEMPLATE)
public class config {
}
```

```java
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.keyvalue.annotation.KeySpace;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@KeySpace("test")
public class Entity implements Serializable {
    @Id
    private String id;
    private String name;
    private String passwd;
}
```

### Repository 用法

```java
import org.springframework.data.keyvalue.repository.KeyValueRepository;

public interface EntityRepository extends KeyValueRepository<Entity, String> {
    // 就像 Spring Data Jpa 一样使用
}
```

### RocksdbTemplate 用法

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RocksdbTemplateTest {
    @Autowired
    private RocksdbTemplate rocksdbTemplate;

    public void test() {
        rocksdbTemplate.insert(new Entity("id", "name", "passwd"));
        rocksdbTemplate.findAll(Entity.class);
    }
}
```

```xml
<dependency>
    <groupId>io.github.jxch</groupId>
    <artifactId>capital-rocksdb-keyvalue-spring-boot-starter</artifactId>
    <version>3.2.5-alpha.1.1.0</version>
</dependency>
```
