package io.github.jxch.capital.rocksdb.adapter;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Objects;
import java.util.function.Function;

@Data
@NoArgsConstructor
@Accessors(chain = true)
public class RocksdbQueryCriteria {
    private Function<String, Boolean> keySpaceMatches;
    private Function<String, Boolean> idMatches;
    private Function<Object, Boolean> valueSpaceMatches;

    public boolean matches(String keySpace, String id, Object value) {
        return (Objects.isNull(keySpaceMatches) || keySpaceMatches.apply(keySpace))
                && (Objects.isNull(idMatches) || idMatches.apply(id))
                && (Objects.isNull(valueSpaceMatches) || valueSpaceMatches.apply(value));
    }
}
