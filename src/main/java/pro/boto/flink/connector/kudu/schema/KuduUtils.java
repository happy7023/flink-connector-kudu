package pro.boto.flink.connector.kudu.schema;


import org.apache.commons.lang3.ClassUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.protolang.utils.Parser;

import java.util.Date;
import java.util.List;

public class KuduUtils {

    public static <T> T parseValue(Object value, Class<T> clazz) throws KuduException {
        T ob = null;
        if(ClassUtils.isAssignable(clazz,Integer.class)){
            ob = (T)Parser.toInteger(value);
        }else if(ClassUtils.isAssignable(clazz,Long.class)){
            ob = (T)Parser.toLong(value);
        }else if(ClassUtils.isAssignable(clazz,Double.class)){
            ob = (T)Parser.toDouble(value);
        }else if(ClassUtils.isAssignable(clazz,Float.class)){
            ob = (T)Parser.toFloat(value);
        }else if(ClassUtils.isAssignable(clazz,Boolean.class)){
            ob = (T)Parser.toBoolean(value);
        }else if(ClassUtils.isAssignable(clazz,Byte.class)){
            //ob = (T)Parser.toBy(value);
        }else if(ClassUtils.isAssignable(clazz,Short.class)){
          //  ob = (T)Parser.toSh(value);
        }else if(ClassUtils.isAssignable(clazz,String.class)){
            ob = (T)Parser.toString(value);
        }else if(ClassUtils.isAssignable(clazz,Date.class)){
            ob = (T)Parser.toDate(value);
        }else{
            throw new KuduException("unknown type");
        }
        return ob;
    }

    public static Class typeToClass(Type type) throws KuduException{
        Class clazz = null;
        switch (type) {
            case STRING:
                clazz = String.class;
                break;
            case FLOAT:
                clazz = Float.class;
                break;
            case INT8:
                clazz = Byte.class;
                break;
            case INT16:
                clazz = Short.class;
                break;
            case INT32:
                clazz = Integer.class;
                break;
            case INT64:
                clazz = Long.class;
                break;
            case DOUBLE:
                clazz = Double.class;
                break;
            case BOOL:
                clazz = Boolean.class;
                break;
            case UNIXTIME_MICROS:
                clazz = Date.class;
                break;
            case BINARY:
                throw new KuduException("not mapped filter");
        }
        return clazz;
    }



    protected static KuduPredicate predicate(KuduFilter filter, Schema kSchema) throws KuduException {
        ColumnSchema column = kSchema.getColumn(filter.getColumn());

        KuduPredicate predicate;

        switch (filter.getFilter()) {
            case IS_IN:
                predicate = KuduPredicate.newInListPredicate(column, (List) filter.getValue());
                break;
            case IS_NULL:
                predicate = KuduPredicate.newIsNullPredicate(column);
                break;
            case IS_NOT_NULL:
                predicate = KuduPredicate.newIsNotNullPredicate(column);
                break;
            default:
                predicate = predicateComparator(column,filter);
                break;
        }
        return predicate;
    }

    private static KuduPredicate predicateComparator(ColumnSchema column, KuduFilter filter) throws KuduException {

        KuduPredicate.ComparisonOp comparison = filter.getFilter().comparator;

        KuduPredicate predicate = null;

        switch (column.getType()) {
            case STRING:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, Parser.toString(filter.getValue()));
                break;
            case FLOAT:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, Parser.toFloat(filter.getValue()));
                break;
            case INT8:
            case INT16:
            case INT32:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, Parser.toInteger(filter.getValue()));
                break;
            case INT64:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, Parser.toLong(filter.getValue()));
                break;
            case DOUBLE:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, Parser.toDouble(filter.getValue()));
                break;
            case BOOL:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, Parser.toBoolean(filter.getValue()));
                break;
            case UNIXTIME_MICROS:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, Parser.toDate(filter.getValue()).getTime());
                break;
            case BINARY:
                throw new KuduException("not mapped filter");
        }
        return predicate;
    }
}
