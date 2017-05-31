package pro.boto.flink.connector.kudu.async;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.log4j.Logger;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduConnector;
import pro.boto.flink.connector.kudu.schema.KuduTable;

public abstract class KuduAsyncFunction<IN,OUT> extends RichAsyncFunction<IN,OUT> {

    protected transient KuduConnector connector;
    protected KuduTable table;

    public KuduAsyncFunction(KuduTable table) throws KuduException {
        if (table == null) {
            throw new IllegalArgumentException("table not valid (null or empty)");
        }
        if (StringUtils.isBlank(table.getMaster())) {
            throw new IllegalArgumentException("host not valid (null or empty)");
        }
        this.table = table;
    }

    @Override
    public void open(Configuration parameters) throws KuduException {
        this.connector = new KuduConnector(table.getMaster());
        connector.createTable(table);
    }

    @Override
    public void close() throws KuduException {
        connector.close();
    }

}