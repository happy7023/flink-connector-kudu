package pro.boto.flink.connector.kudu.schema;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import pro.boto.protolang.domain.ProtoObject;

public class KuduColumn extends ProtoObject<KuduColumn> {

    protected final String name;
    protected final Type type;
    protected final boolean key;
    protected final boolean rangeKey;
    protected final boolean nullable;
    protected final Object defaultValue;
    protected final int blockSize;
    protected final Encoding encoding;
    protected final Compression compression;

    private KuduColumn(String name, Type type,
                       boolean key, boolean rangeKey, boolean nullable,
                       Object defaultValue, int blockSize,
                       Encoding encoding, Compression compression) {
        this.name = name;
        this.type = type;
        this.key = key;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.blockSize = blockSize;
        this.encoding = encoding;
        this.compression = compression;
        this.rangeKey = rangeKey;
    }
    
    protected ColumnSchema columnSchema() {
        return new ColumnSchema.ColumnSchemaBuilder(name,type)
                    .key(key)
                    .nullable(nullable)
                    .defaultValue(defaultValue)
                    .desiredBlockSize(blockSize)
                    .encoding(encoding.encode)
                    .compressionAlgorithm(compression.algorithm)
                    .build();
    }

    public static class Builder {
        private final String name;
        private final Type type;
        private boolean key = false;
        private boolean rangeKey = false;
        private boolean nullable = false;
        private Object defaultValue = null;
        private int blockSize = 0;
        private KuduColumn.Encoding encoding = Encoding.AUTO;
        private KuduColumn.Compression compression = Compression.DEFAULT;

        private Builder(String name, Type type) {
            this.name = name;
            this.type = type;
        }

        public static KuduColumn.Builder create(String name, Type type) {
            return new Builder(name, type);
        }

        public KuduColumn.Builder key(boolean key) {
            this.key = key;
            return this;
        }

        public KuduColumn.Builder rangeKey(boolean rangeKey) {
            this.rangeKey = rangeKey;
            return this;
        }

        public KuduColumn.Builder nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public KuduColumn.Builder defaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public KuduColumn.Builder desiredBlockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public KuduColumn.Builder encoding(KuduColumn.Encoding encoding) {
            this.encoding = encoding;
            return this;
        }

        public KuduColumn.Builder compressionAlgorithm(KuduColumn.Compression compression) {
            this.compression = compression;
            return this;
        }

        public KuduColumn build() {
            return new KuduColumn(this.name, this.type, this.key, this.rangeKey, this.nullable, this.defaultValue, this.blockSize, this.encoding, this.compression);
        }
    }

    public enum Compression {
        UNKNOWN(ColumnSchema.CompressionAlgorithm.UNKNOWN),
        DEFAULT(ColumnSchema.CompressionAlgorithm.DEFAULT_COMPRESSION),
        WITHOUT(ColumnSchema.CompressionAlgorithm.NO_COMPRESSION),
        SNAPPY(ColumnSchema.CompressionAlgorithm.SNAPPY),
        LZ4(ColumnSchema.CompressionAlgorithm.LZ4),
        ZLIB(ColumnSchema.CompressionAlgorithm.ZLIB);

        final ColumnSchema.CompressionAlgorithm algorithm;

        Compression(ColumnSchema.CompressionAlgorithm algorithm) {
            this.algorithm = algorithm;
        }

    }

    public enum Encoding {
        UNKNOWN(ColumnSchema.Encoding.UNKNOWN),
        AUTO(ColumnSchema.Encoding.AUTO_ENCODING),
        PLAIN(ColumnSchema.Encoding.PLAIN_ENCODING),
        PREFIX(ColumnSchema.Encoding.PREFIX_ENCODING),
        GROUP_VARINT(ColumnSchema.Encoding.GROUP_VARINT),
        RLE(ColumnSchema.Encoding.RLE),
        DICT(ColumnSchema.Encoding.DICT_ENCODING),
        BIT_SHUFFLE(ColumnSchema.Encoding.BIT_SHUFFLE);

        final ColumnSchema.Encoding encode;

        Encoding(ColumnSchema.Encoding encode) {
            this.encode = encode;
        }

    }
}
