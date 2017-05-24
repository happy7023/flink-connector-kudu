package pro.boto.flink.connector.kudu.schema;

import org.apache.kudu.client.KuduPredicate;

import java.util.List;


public class KuduFilter {

    public enum FilterType {
        GREATER(KuduPredicate.ComparisonOp.GREATER),
        GREATER_EQUAL(KuduPredicate.ComparisonOp.GREATER_EQUAL),
        EQUAL(KuduPredicate.ComparisonOp.EQUAL),
        LESS(KuduPredicate.ComparisonOp.LESS),
        LESS_EQUAL(KuduPredicate.ComparisonOp.LESS_EQUAL),
        IS_NOT_NULL(null),
        IS_NULL(null),
        IS_IN(null);

        final KuduPredicate.ComparisonOp comparator;

        FilterType(KuduPredicate.ComparisonOp comparator) {
            this.comparator = comparator;
        }

    }


    private String column;
    private FilterType filter;
    private Object value;

    protected KuduFilter(String column,FilterType filter,Object value){
        this.column = column;
        this.filter = filter;
        this.value = value;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    public FilterType getFilter() {
        return filter;
    }

    public static class Builder {
        private String column;
        private FilterType filter;
        private Object value;


        private Builder(String column) {
            this.column = column;
        }

        public static KuduFilter.Builder create(String column) {
            return new KuduFilter.Builder(column);
        }

        public KuduFilter.Builder greaterThan(Object value) {
            this.filter = FilterType.GREATER;
            this.value = value;
            return this;
        }

        public KuduFilter.Builder lessThan(Object value) {
            this.filter = FilterType.LESS;
            this.value = value;
            return this;
        }

        public KuduFilter.Builder equalTo(Object value) {
            this.filter = FilterType.EQUAL;
            this.value = value;
            return this;
        }

        public KuduFilter.Builder greaterOrEqualTo(Object value) {
            this.filter = FilterType.GREATER_EQUAL;
            this.value = value;
            return this;
        }

        public KuduFilter.Builder lessOrEqualTo(Object value) {
            this.filter = FilterType.LESS_EQUAL;
            this.value = value;
            return this;
        }

        public KuduFilter.Builder isNotNull() {
            this.filter = FilterType.IS_NOT_NULL;
            return this;
        }

        public KuduFilter.Builder isNull() {
            this.filter = FilterType.IS_NULL;
            return this;
        }

        public KuduFilter.Builder isIn(List values) {
            this.filter = FilterType.IS_IN;
            this.value = values;
            return this;
        }

        public KuduFilter build() {
            return new KuduFilter(this.column, this.filter, this.value);
        }
    }

}
