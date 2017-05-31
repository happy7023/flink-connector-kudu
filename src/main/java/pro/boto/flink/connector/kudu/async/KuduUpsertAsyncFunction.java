package pro.boto.flink.connector.kudu.async;

import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.protolang.domain.ProtoObject;

import java.util.Collections;

public class KuduUpsertAsyncFunction extends KuduAsyncFunction<KuduRow,KuduRow> {

    public KuduUpsertAsyncFunction(KuduTable table) throws KuduException {
        super(table);
    }

    @Override
    public void asyncInvoke(KuduRow row, AsyncCollector<KuduRow> asyncCollector) throws KuduException {
        connector.upsert(table, row);
        asyncCollector.collect(Collections.singleton(row));
    }

}