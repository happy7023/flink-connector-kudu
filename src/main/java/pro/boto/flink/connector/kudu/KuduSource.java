package pro.boto.flink.connector.kudu;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.log4j.Logger;
import pro.boto.flink.connector.kudu.schema.KuduConnector;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;

import java.io.Serializable;
import java.util.List;

public class KuduSource extends RichSourceFunction<KuduRow> implements Serializable {

    private final static Logger logger = Logger.getLogger(KuduSource.class);

    private transient KuduConnector connector;
    private KuduTable table;

    public KuduSource(KuduTable table) throws KuduException {
        if (table == null) {
            throw new IllegalArgumentException("table not valid (null or empty)");
        }
        if (StringUtils.isBlank(table.getMaster())) {
            throw new IllegalArgumentException("host not valid (null or empty)");
        }
        this.table = table;

        connect();
        connector.createTable(table);
        logger.info("source created");
    }


    private void connect() throws KuduException {
        if(this.connector==null) {
            this.connector = new KuduConnector(table.getMaster());
        }
    }

    @Override
    public void run(SourceContext<KuduRow> sourceContext) throws Exception {
        connect();
        List<KuduRow> rows = connector.read(table);
        for (KuduRow row: rows) {
            sourceContext.collect(row);
        }
    }

    @Override
    public void cancel() {

    }
}