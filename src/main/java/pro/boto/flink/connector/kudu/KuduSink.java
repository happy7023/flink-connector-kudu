package pro.boto.flink.connector.kudu;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;
import pro.boto.flink.connector.kudu.schema.KuduConnector;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;

import java.io.Serializable;

public class KuduSink extends RichSinkFunction<KuduRow> implements Serializable {

    private final static Logger logger = Logger.getLogger(KuduSink.class);

    private transient KuduConnector connector;
    private KuduTable table;

    public KuduSink (KuduTable table) throws KuduException {
        if (table == null) {
            throw new IllegalArgumentException("table not valid (null or empty)");
        }
        if (StringUtils.isBlank(table.getMaster())) {
            throw new IllegalArgumentException("host not valid (null or empty)");
        }
        this.table = table;

        connect();
        connector.createTable(table);
        logger.info("sink created");
    }

    @Override
    public void invoke(KuduRow row) throws KuduException {
        connect();
        connector.upsert(table, row);
        logger.info("Inserted the Row: | " + row + "at the table \"" + this.table.getName() + "\"");
    }

    private void connect() throws KuduException {
        if(this.connector==null) {
            this.connector = new KuduConnector(table.getMaster());
        }
    }

}