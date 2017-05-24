package pro.boto.flink.connector.kudu.schema;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import pro.boto.protolang.domain.ProtoObject;

import java.util.ArrayList;
import java.util.List;

public class KuduTable extends ProtoObject<KuduTable> {

    public enum Mode {INSERT,UPDATE,UPSERT}
    private final static Integer DEFAULT_REPLICAS = 3;
    private final static boolean DEFAULT_CREATE_IF_NOT_EXIST = false;

    private String master;
    private Integer replicas;
    private String name;
    private Mode mode;
    private boolean createIfNotExist;
    private List<KuduColumn> columns;

    private KuduTable(){}

    public String getName() {
        return name;
    }

    public String getMaster() {
        return master;
    }

    public Schema getSchema() {
        if(hasNotColumns()) return null;
        List<ColumnSchema> schemaColumns = new ArrayList<>();
        for(KuduColumn column : columns){
            schemaColumns.add(column.columnSchema());
        }
        return new Schema(schemaColumns);
    }

    public boolean createIfNotExist() {
        return createIfNotExist;
    }

    public CreateTableOptions getCreateTableOptions() {
        CreateTableOptions options = new CreateTableOptions();
        if(replicas!=null){
            options.setNumReplicas(replicas);
        }
        if(hasColummns()) {
            List<String> rangeKeys = new ArrayList<>();
            for(KuduColumn column : columns){
                if(column.rangeKey){
                    rangeKeys.add(column.name);
                }
            }
            options.setRangePartitionColumns(rangeKeys);
        }
        return options;
    }

    public boolean hasNotColumns(){
        return !hasColummns();
    }
    public boolean hasColummns(){
        return (columns!=null && columns.size()>0);
    }

    public static class Builder {
        private List<KuduColumn> columns;
        private String name;
        private String master;
        private Integer replicas;
        private Mode mode;
        private Boolean createIfNotExist;

        private Builder(String master, String name, boolean createIfNotExist, int replicas) {
            this.master = master;
            this.name = name;
            this.replicas = replicas;
            this.createIfNotExist = createIfNotExist;
            this.columns = new ArrayList<>();
            this.mode = Mode.UPSERT;
        }

        public static KuduTable.Builder create(String master, String name) {
            return create(master, name, DEFAULT_CREATE_IF_NOT_EXIST);
        }
        public static KuduTable.Builder create(String master, String name, boolean createIfNotExist) {
            return create(master, name, createIfNotExist, DEFAULT_REPLICAS);
        }
        public static KuduTable.Builder create(String master, String name, boolean createIfNotExist, int replicas) {
            return new Builder(master, name, createIfNotExist, replicas);
        }

        public static KuduTable.Builder open(String master, String name) {
            return new Builder(master, name, DEFAULT_CREATE_IF_NOT_EXIST, DEFAULT_REPLICAS);
        }

        public KuduTable.Builder mode(Mode tableMode) {
            if(tableMode == null) return this;
            this.mode = tableMode;
            return this;
        }

        public KuduTable.Builder column(KuduColumn column) {
            if(column==null || column.hasNotInfo()) return this;
            this.columns.add(column);
            return this;
        }

        public KuduTable build() {
            KuduTable table = new KuduTable();
            table.master = this.master;
            table.name = this.name;
            table.createIfNotExist = createIfNotExist;
            table.replicas = replicas;
            table.mode = mode;
            table.columns = columns;
            return table;
        }
    }
}
