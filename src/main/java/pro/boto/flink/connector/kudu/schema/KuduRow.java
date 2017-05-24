package pro.boto.flink.connector.kudu.schema;

import com.fasterxml.jackson.databind.util.ClassUtil;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.protolang.domain.ProtoMap;
import pro.boto.protolang.domain.ProtoObject;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

public class KuduRow extends ProtoMap<String,Object> {

    public KuduRow(Map<String, Object> values) throws KuduException {
        super(values);
    }

    public KuduRow(Object object) throws KuduException {
        super();
        for (Class<?> c = object.getClass(); c != null; c = c.getSuperclass()) {
            Field[] fields = c.getDeclaredFields();
            for (Field cField : fields) {
                try {
                    if (!Modifier.isStatic(cField.getModifiers())
                            && !Modifier.isTransient(cField.getModifiers())) {
                        cField.setAccessible(true);
                        super.add(cField.getName(), cField.get(object));
                    }
                } catch (IllegalAccessException e) {
                    throw new KuduException(e.getLocalizedMessage(), e);
                }
            }
        }
    }

    public <P extends ProtoObject> P blind(Class<P> clazz) throws KuduException {
        try {
            P o = ClassUtil.createInstance(clazz,true);


            for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
                Field[] fields = c.getDeclaredFields();
                for (Field cField : fields) {
                    if(super.contains(cField.getName())
                            && !Modifier.isStatic(cField.getModifiers())
                            && !Modifier.isTransient(cField.getModifiers())) {

                        cField.setAccessible(true);
                        cField.set(o, super.obtain(cField.getName()));
                    }
                }
            }

            return o;
        } catch (Exception e) {
            throw new KuduException(e.getLocalizedMessage(), e);
        }
    }

}
