package pro.boto.flink.connector.kudu.async;

import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.protolang.domain.ProtoObject;

import java.util.Collections;

public class KuduUpsertAsyncFunction<T extends ProtoObject<T>> extends KuduAsyncFunction<T,T> {

    public KuduUpsertAsyncFunction(KuduTable table) throws KuduException {
        super(table);
        LOG.info("upsert created");
    }

    @Override
    public void asyncInvoke(T proto, AsyncCollector<T> asyncCollector) throws KuduException {
        connector.upsert(table, new KuduRow(proto));
        asyncCollector.collect(Collections.singleton(proto));
    }

}