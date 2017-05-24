package pro.boto.flink.connector.kudu.function;

import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduFilter;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.protolang.domain.ProtoObject;

import java.util.ArrayList;
import java.util.List;

public class KuduReadFunction<T extends ProtoObject> extends KuduFunction<List<KuduFilter>,List<T>>  {

    private Class<T> clazz;

    public KuduReadFunction(KuduTable table, Class<T> clazz) throws KuduException {
        super(table);
        this.clazz = clazz;
        LOG.info("reader created");
    }


    @Override
    public List<T> map(List<KuduFilter> filters) throws KuduException {
        connect();

        List<KuduRow> rows = connector.read(table, filters);
        LOG.info("reading rows on table \"" + this.table.getName() +"\" with filters "+filters);

        List<T> values = new ArrayList<>();
        for (KuduRow row: rows) {
            values.add(row.blind(clazz));
        }

        return values;

    }

}