package pro.boto.flink.connector.kudu.function;

import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.protolang.domain.ProtoObject;

public class KuduUpsertFunction<T extends ProtoObject<T>> extends KuduFunction<T,T> {

    public KuduUpsertFunction(KuduTable table) throws KuduException {
        super(table);
        LOG.info("upsert created");
    }

    @Override
    public T map(T proto) throws Exception {
        connect();
        connector.upsert(table, new KuduRow(proto));

        LOG.info("upsert into table \"" + this.table.getName() + "\" the row: " + proto);
        return proto;
    }

}