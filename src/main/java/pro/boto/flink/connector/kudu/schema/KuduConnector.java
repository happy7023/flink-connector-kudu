package pro.boto.flink.connector.kudu.schema;

import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.log4j.Logger;
import pro.boto.protolang.utils.Parser;

import java.util.*;

public class KuduConnector implements AutoCloseable {

    private final static Logger LOG = Logger.getLogger(KuduConnector.class);

    private KuduClient client;


    public KuduConnector(String kuduHost) throws pro.boto.flink.connector.kudu.KuduException {
        if(StringUtils.isBlank(kuduHost)) {
            throw new pro.boto.flink.connector.kudu.KuduException("host cannot be empty");
        }
        this.client = new KuduClient.KuduClientBuilder(kuduHost).build();
        if (client == null){
            throw new pro.boto.flink.connector.kudu.KuduException("cant establish connection with host: "+ kuduHost);
        }
    }

    @Override
    public void close() throws pro.boto.flink.connector.kudu.KuduException {
        try {
            client.close();
        }catch (Exception e){
            throw new pro.boto.flink.connector.kudu.KuduException("something goes worng closing connection");
        }
    }

    public boolean createTable (KuduTable table) throws pro.boto.flink.connector.kudu.KuduException {
        try {
            if(client.tableExists(table.getName())) return false;

            if(!table.createIfNotExist()) {
                throw new pro.boto.flink.connector.kudu.KuduException("table is market to not be created");
            }

            if(table.hasNotColumns()){
                throw new pro.boto.flink.connector.kudu.KuduException("table columns must be defined");
            }

            client.createTable(table.getName(),
                               table.getSchema(),
                               table.getCreateTableOptions());
            return true;
        }catch (org.apache.kudu.client.KuduException e){
            throw new pro.boto.flink.connector.kudu.KuduException(e.getLocalizedMessage(), e);
        }
    }

    public boolean deleteTable(KuduTable table) throws pro.boto.flink.connector.kudu.KuduException {
        try {
            if(!client.tableExists(table.getName())) return false;

            client.deleteTable(table.getName());
            return true;
        }catch (org.apache.kudu.client.KuduException e){
            throw new pro.boto.flink.connector.kudu.KuduException(e.getLocalizedMessage(), e);
        }
    }


    private org.apache.kudu.client.KuduTable openTable(KuduTable table) throws pro.boto.flink.connector.kudu.KuduException {
        try {
            if(client.tableExists(table.getName())){
                return client.openTable(table.getName());
            }
            if(!table.createIfNotExist()) {
                throw new pro.boto.flink.connector.kudu.KuduException("table "+table.getName()+" not exists. and is market to not be created");
            }
            if(createTable(table)){
                return client.openTable(table.getName());
            }
            throw new pro.boto.flink.connector.kudu.KuduException("table "+table.getName()+" not exists. must be created first");
        }catch (org.apache.kudu.client.KuduException e){
            throw new pro.boto.flink.connector.kudu.KuduException(e.getLocalizedMessage(), e);
        }
    }

    public List<KuduRow> read(KuduTable table, List<KuduFilter> filters) throws pro.boto.flink.connector.kudu.KuduException {
        try {
            org.apache.kudu.client.KuduTable kTable = openTable(table);
            Schema kSchema = kTable.getSchema();

            KuduScanner.KuduScannerBuilder scannerBuilder = client.newScannerBuilder(kTable);

            for(KuduFilter filter : filters) {
                scannerBuilder.addPredicate(KuduUtils.predicate(filter, kSchema));
            }

            List<KuduRow> rows = new ArrayList<>();

            KuduScanner scanner = scannerBuilder.build();
            while (scanner.hasMoreRows()) {
                RowResultIterator data = scanner.nextRows();
                while (data.hasNext()) {
                    RowResult row = data.next();
                    Map<String, Object> values = new HashMap<>();

                    for (ColumnSchema col : kSchema.getColumns()) {
                        String colName = col.getName();
                        if(row.isNull(colName)) {
                            values.put(colName, null);
                        }else{
                            switch (col.getType()) {
                                case STRING:
                                    values.put(colName, row.getString(colName));
                                    break;
                                case FLOAT:
                                    values.put(colName, row.getFloat(colName));
                                    break;
                                case INT8:
                                    values.put(colName, row.getByte(colName));
                                    break;
                                case INT16:
                                    values.put(colName, row.getShort(colName));
                                    break;
                                case INT32:
                                    values.put(colName, row.getInt(colName));
                                    break;
                                case INT64:
                                    values.put(colName, row.getLong(colName));
                                    break;
                                case DOUBLE:
                                    values.put(colName, row.getDouble(colName));
                                    break;
                                case BOOL:
                                    values.put(colName, row.getBoolean(colName));
                                    break;
                                case UNIXTIME_MICROS:
                                    values.put(colName, new Date(row.getLong(colName)));
                                    break;
                                case BINARY:
                                    throw new pro.boto.flink.connector.kudu.KuduException("not mapped filter");
                            }
                        }
                    }
                    rows.add(new KuduRow(values));
                }
            }
            return rows;
        }catch (org.apache.kudu.client.KuduException e){
            throw new pro.boto.flink.connector.kudu.KuduException(e.getLocalizedMessage(), e);
        }
    }



    public void upsert(KuduTable table, KuduRow row) throws pro.boto.flink.connector.kudu.KuduException {
        try {
            org.apache.kudu.client.KuduTable kTable = openTable(table);
            Upsert insert = kTable.newUpsert();
            PartialRow partialRow = insert.getRow();

            Map<String, Object> values = row.obtain();

            for(ColumnSchema column : kTable.getSchema().getColumns()) {
                String columnName = column.getName();
                if(values.containsKey(columnName) && values.get(columnName)!=null){
                    switch (column.getType()){
                        case STRING:
                            partialRow.addString(columnName, Parser.toString(values.get(columnName)));
                            break;
                        case FLOAT:
                            partialRow.addFloat(columnName, (Float)values.get(columnName));
                            break;
                        case INT8:
                            partialRow.addByte(columnName, (Byte) values.get(columnName));
                            break;
                        case INT16:
                            partialRow.addShort(columnName, (Short)values.get(columnName));
                            break;
                        case INT32:
                            partialRow.addInt(columnName, Parser.toInteger(values.get(columnName)));
                            break;
                        case INT64:
                            partialRow.addLong(columnName, Parser.toLong(values.get(columnName)));
                            break;
                        case DOUBLE:
                            partialRow.addDouble(columnName, Parser.toDouble(values.get(columnName)));
                            break;
                        case BOOL:
                            partialRow.addBoolean(columnName,Parser.toBoolean(values.get(columnName)));
                            break;
                        case UNIXTIME_MICROS:
                            Date date = Parser.toDate(values.get(columnName));
                            if(date == null) break;
                            // *1000 to correctly create date on kudu
                            partialRow.addLong(columnName, date.getTime()*1000);
                            break;
                        case BINARY:
                            throw new pro.boto.flink.connector.kudu.KuduException("not mapped type");
                    }
                }
            }

            KuduSession session = client.newSession();
            session.apply(insert);
            session.flush();
            session.close();
        }catch (org.apache.kudu.client.KuduException e){
            throw new pro.boto.flink.connector.kudu.KuduException(e.getLocalizedMessage(), e);
        }
    }

/*
    public List<KuduRow> read(KuduTable table)throws KuduException {
        try {
            List<KuduRow> rows = new ArrayList<>();
            KuduScanner scanner = client.newScannerBuilder(openTable(table)).build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    rows.add(new Rating(result.getInt("userId"),
                                           result.getInt("estateId"),
                                           result.getString("rating")));
                }
            }
            return rows;
        }catch (org.apache.kudu.client.KuduException e){
            throw new KuduException(e.getLocalizedMessage(), e);
        }
    }
*/
}
