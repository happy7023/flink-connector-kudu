package pro.boto.flink.connector.kudu;

public class KuduException extends Exception {

    public KuduException(String msg) {
        super(msg);
    }

    public KuduException(String msg, Throwable e) {
        super(msg, e);
    }
}
