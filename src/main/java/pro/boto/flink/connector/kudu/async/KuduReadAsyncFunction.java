package pro.boto.flink.connector.kudu.async;

import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduFilter;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.protolang.domain.ProtoObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KuduReadAsyncFunction<T extends ProtoObject> extends KuduAsyncFunction<List<KuduFilter>,List<T>> {

    private Class<T> clazz;

    public KuduReadAsyncFunction(KuduTable table, Class<T> clazz) throws KuduException {
        super(table);
        this.clazz = clazz;
        LOG.info("reader created");
    }

    @Override
    public void asyncInvoke(List<KuduFilter> filters, AsyncCollector<List<T>> asyncCollector) throws KuduException {
        List<KuduRow> rows = connector.read(table, filters);

        List<T> values = new ArrayList<>();
        for (KuduRow row: rows) {
            values.add(row.blind(clazz));
        }

        asyncCollector.collect(Collections.singleton(values));
    }
}