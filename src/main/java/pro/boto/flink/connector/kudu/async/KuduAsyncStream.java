package pro.boto.flink.connector.kudu.async;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduFilter;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.protolang.domain.ProtoObject;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class KuduAsyncStream {

    private static final Integer TIMEOUT = 1;
    private static final TimeUnit TIMEUNIT = TimeUnit.MINUTES;

    public static SingleOutputStreamOperator createUnordered(DataStream stream, KuduAsyncFunction function){
        return AsyncDataStream.unorderedWait(stream, function, TIMEOUT, TIMEUNIT);
    }

    public static SingleOutputStreamOperator<KuduRow> unorderedUpsert(DataStream<KuduRow> stream, KuduTable table) throws KuduException {
        return createUnordered(stream, new KuduUpsertAsyncFunction(table));
    }

    public static SingleOutputStreamOperator<KuduRow> unorderedReader(DataStream<List<KuduFilter>> stream, KuduTable table) throws KuduException {
        return createUnordered(stream, new KuduReadAsyncFunction(table));
    }
}
