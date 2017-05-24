package pro.boto.flink.connector.kudu.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.log4j.Logger;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduConnector;
import pro.boto.flink.connector.kudu.schema.KuduFilter;
import pro.boto.flink.connector.kudu.schema.KuduTable;

import java.util.List;

abstract class KuduFunction<IN,OUT> implements MapFunction<IN,OUT> {
    protected final Logger LOG = Logger.getLogger(this.getClass());

    protected transient KuduConnector connector;
    protected KuduTable table;

    public KuduFunction(KuduTable table) throws KuduException {
        if (table == null) {
            throw new IllegalArgumentException("table not valid (null or empty)");
        }
        if (StringUtils.isBlank(table.getMaster())) {
            throw new IllegalArgumentException("host not valid (null or empty)");
        }
        this.table = table;
        connect();
        connector.createTable(table);
    }

    protected void connect() throws KuduException {
        if(connector!=null) return;
        this.connector = new KuduConnector(table.getMaster());
    }

}