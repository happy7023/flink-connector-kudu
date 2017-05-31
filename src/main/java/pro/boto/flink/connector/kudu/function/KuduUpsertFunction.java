package pro.boto.flink.connector.kudu.function;

import org.apache.flink.util.Collector;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.protolang.domain.ProtoObject;

public class KuduUpsertFunction extends KuduFunction<KuduRow,KuduRow> {

    public KuduUpsertFunction(KuduTable table) throws KuduException {
        super(table);
    }

    @Override
    public KuduRow map(KuduRow row) throws Exception {
        connect();
        connector.upsert(table, row);
        return row;
    }

    @Override
    public void flatMap(KuduRow row, Collector<KuduRow> collector) throws Exception {
        connect();
        connector.upsert(table, row);
        collector.collect(row);
    }
}