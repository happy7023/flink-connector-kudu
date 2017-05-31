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

public class KuduReadAsyncFunction extends KuduAsyncFunction<List<KuduFilter>,KuduRow> {


    public KuduReadAsyncFunction(KuduTable table) throws KuduException {
        super(table);
    }

    @Override
    public void asyncInvoke(List<KuduFilter> filters, AsyncCollector<KuduRow> asyncCollector) throws KuduException {
        List<KuduRow> rows = connector.read(table, filters);
        rows.forEach(row -> asyncCollector.collect(Collections.singleton(row)));
    }
}