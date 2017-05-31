package pro.boto.flink.connector.kudu.function;

import org.apache.flink.util.Collector;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduFilter;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.protolang.domain.ProtoObject;

import java.util.ArrayList;
import java.util.List;

public class KuduReadFunction extends KuduFunction<List<KuduFilter>,KuduRow>  {

    public KuduReadFunction(KuduTable table) throws KuduException {
        super(table);
    }


    @Override
    public KuduRow map(List<KuduFilter> filters) throws KuduException {
        connect();

        List<KuduRow> rows = connector.read(table, filters);

        if(rows == null || rows.isEmpty()) return null;

        if(!rows.isEmpty() && rows.size()>1){
            throw new KuduException("only expected one row and obtain "+rows.size());
        }

        return rows.get(0);

    }

    @Override
    public void flatMap(List<KuduFilter> filters, Collector<KuduRow> collector) throws Exception {
        connect();

        List<KuduRow> rows = connector.read(table, filters);

        rows.forEach(row -> collector.collect(row));
    }
}