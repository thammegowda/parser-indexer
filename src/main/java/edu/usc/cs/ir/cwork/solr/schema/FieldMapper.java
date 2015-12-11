package edu.usc.cs.ir.cwork.solr.schema;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Field Name Mapper maps document field names to solr dynamic schema based on configuration
 *
 * @author Thamme Gowda
 * @since Oct. 27, 2015
 */
public class FieldMapper {

    private static final Logger LOG = LoggerFactory.getLogger(FieldMapper.class);
    public static final String SCHEMA_MAP_FILE = "solr-schema-map.json";
    public static final String KEY_OVERRIDES = "overrides";
    public static final String KEY_MULTI_VAL_SUFFIX = "multiValSuffix";
    public static final String DEFAULT_MULTI_VAL_SUFFIX = "s";
    public static final String KEY_TYPE_SUFFIX = "typeSuffix";

    private static Map<Class, Class> PRIM_MAP = new HashMap<>();
    static {
        PRIM_MAP.put(boolean.class, Boolean.class);
        PRIM_MAP.put(byte.class, Byte.class);
        PRIM_MAP.put(char.class, Character.class);
        PRIM_MAP.put(double.class, Double.class);
        PRIM_MAP.put(float.class, Float.class);
        PRIM_MAP.put(int.class, Integer.class);
        PRIM_MAP.put(long.class, Long.class);
        PRIM_MAP.put(short.class, Short.class);
    }

    //type map
    public final Map<Class, String> typeSuffix = new HashMap<>();

    public String multiValSuffix = DEFAULT_MULTI_VAL_SUFFIX;
    public final Map<String, String> overrides = new HashMap<>();

    private StringEvaluator evaluator = new StringEvaluator();
    private boolean failOnError = true;

    /**
     * Creates a Field name mapper by readin rules from default config file {@code SCHEMA_MAP_FILE} in class loader
     * @throws RuntimeException when config is invalid
     */
    public static FieldMapper create(){
        try (InputStream stream = FieldMapper.class.getClassLoader().getResourceAsStream(SCHEMA_MAP_FILE)){
            if (stream == null) {
                throw new RuntimeException("Couldn't find config file in class path : " + SCHEMA_MAP_FILE);
            }
            return new FieldMapper(stream);
        } catch (IOException|ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a field name mapper by reading config from the argument conf stream
     * @param jConfStream JSON config stream
     * @throws IOException
     * @throws ParseException when the config is invalid
     */
    public FieldMapper(InputStream jConfStream) throws IOException, ParseException {
        try (InputStreamReader reader = new InputStreamReader(jConfStream)) {
            Map jConf = (JSONObject) new JSONParser().parse(reader);
            if (jConf.containsKey(KEY_OVERRIDES)) {
                ((Map) jConf.get(KEY_OVERRIDES)).forEach((k,v) -> overrides.put(k.toString(), v.toString()));
            }
            if (jConf.containsKey(KEY_MULTI_VAL_SUFFIX)) {
                this.multiValSuffix = jConf.getOrDefault(KEY_MULTI_VAL_SUFFIX, DEFAULT_MULTI_VAL_SUFFIX).toString();
            }
            if (jConf.containsKey(KEY_TYPE_SUFFIX)) {
                ((Map) jConf.get(KEY_TYPE_SUFFIX)).forEach((k,v) -> {
                    try {
                        typeSuffix.put(Class.forName(k.toString()), v.toString());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException("Class " + k.toString() + " is invalid or unknown");
                    }
                });
            }
        }
    }


    /**
     * Maps field name to a dynamic field name based on value content type
     * The map function takes (input field name, value type,  configs, overrides) into consideration
     * for deciding field names
     * @param fieldName name of field.
     * @param value value
     * @return mapped solr field name. returns {@code null} when mapping fails
     * @throws IllegalStateException when {@code this.failOnError} is set and an error occurs
     */
    public String mapField(String fieldName, Object value) {
        if (overrides.containsKey(fieldName)) {
            return overrides.get(fieldName);
        }
        if (value == null) {
            return null; // null cant be mapped
        }
        Class<?> valType = value.getClass();

        boolean multiValued = false;
        if (valType.isArray()) {
            multiValued = true;
            valType = valType.getComponentType();
            //it could be primitive type, so mapping to Boxed Type
            valType = PRIM_MAP.getOrDefault(valType, valType);
        } else if (Collection.class.isAssignableFrom(valType)){
            multiValued = true;
            Collection items = (Collection) value;
            if (items.isEmpty()) {
                return null;
                //cant determine an empty collection
            }
            valType = items.iterator().next().getClass();
        }
        String suffix = typeSuffix.get(valType);
        if (suffix == null) {
            LOG.warn("{} type is not mapped, field name = {}", valType.getName(), fieldName);
            if (failOnError) {
                throw new IllegalStateException(valType + " type is not known");
            }
            return null;
        }

        if (multiValued) {
            suffix += multiValSuffix;
        }
        //is it already mapped
        if (fieldName.endsWith(suffix)) {
            return fieldName;
        }
        fieldName = fieldName.toLowerCase().replaceAll("\\s+", "");
        return fieldName + suffix;
    }

    /**
     * Maps fields to dynamic schema
     * @param fields fields to be mapped
     * @param eval cast the value type
     * @return mapped fields
     */
    public Map<String, Object> mapFields(Map<String, Object> fields, boolean eval){
        Map<String, Object> result = new HashMap<>();

        fields.forEach((k,v) ->{
            if (eval && evaluator.canEval(v)) {
                v = evaluator.eval(v);
            }
            result.put(mapField(k, v), v);
        });
        return result;
    }

    public static void main(String[] args) throws ClassNotFoundException {
        System.out.println(Integer.TYPE);
        Class<?>x = int.class;
        System.out.println(x);

    }
}
