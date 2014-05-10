/**
 * JSON SerDe for Hive
 */
package org.apache.hadoop.hive.contrib.serde2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.internal.JsonReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JSON SerDe for Hive
 * <p/>
 * This SerDe can be used to read data in JSON format. For example, if your JSON
 * files had the following contents:
 * <p/>
 * <pre>
 * {"field1":"data1","field2":100,"field3":"more data1"}
 * {"field1":"data2","field2":200,"field3":"more data2"}
 * {"field1":"data3","field2":300,"field3":"more data3"}
 * {"field1":"data4","field2":400,"field3":"more data4"}
 * </pre>
 * <p/>
 * The following steps can be used to read this data:
 * <ol>
 * <li>Build this project using <code>ant build</code></li>
 * <li>Copy <code>hive-json-serde.jar</code> to the Hive server</li>
 * <li>Inside the Hive client, run
 * <p/>
 * <pre>
 * ADD JAR /home/hadoop/hive-json-serde.jar;
 * </pre>
 * <p/>
 * </li>
 * <li>Create a table that uses files where each line is JSON object
 * <p/>
 * <pre>
 * CREATE EXTERNAL TABLE IF NOT EXISTS my_table (
 *    field1 string, field2 int, field3 string
 * )
 * ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'
 * LOCATION '/my_data/my_table/';
 * </pre>
 * <p/>
 * </li>
 * <li>Copy your JSON files to <code>/my_data/my_table/</code>. You can now
 * select data using normal SELECT statements
 * <p/>
 * <pre>
 * SELECT * FROM my_table LIMIT 10;
 * </pre>
 * <p/>
 * <p/>
 * </li>
 * </ol>
 * <p/>
 * The table does not have to have the same columns as the JSON files, and
 * vice-versa. If the table has a column that does not exist in the JSON object,
 * it will have a NULL value. If the JSON file contains fields that are not
 * columns in the table, they will be ignored and not visible to the table.
 *
 * @author Peter Sankauskas
 * @see <a href="http://code.google.com/p/hive-json-serde/">hive-json-serde on
 * Google Code</a>
 */
public class JsonSerde implements SerDe {
    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(JsonSerde.class);

    /**
     * JsonReader to parse json very quickly
     */
    private final JsonReader jsonReader = new JsonReader();

    /**
     * The number of columns in the table this SerDe is being used with
     */
    private int numColumns;

    /**
     * List of column names in the table
     */
    private List<String> columnNames;

    /**
     * An ObjectInspector to be used as meta-data about a deserialized row
     */
    private StructObjectInspector rowOI;

    /**
     * List of row objects
     */
    private ArrayList<Object> row;

    /**
     * List of column type information
     */
    private List<TypeInfo> columnTypes;

    private Properties tbl;
    private Map<String, JsonPath> colNameJsonPathMap = null;

    /**
     * Initialize this SerDe with the system properties and table properties
     */
    @Override
    public void initialize(Configuration sysProps, Properties tblProps)
            throws SerDeException {
        LOG.debug("Initializing JsonSerde");

        tbl = tblProps;

        // Get the names of the columns for the table this SerDe is being used
        // with
        String columnNameProperty = tblProps
                .getProperty(Constants.LIST_COLUMNS);
        columnNames = Arrays.asList(columnNameProperty.split(","));

        // Convert column types from text to TypeInfo objects
        String columnTypeProperty = tblProps
                .getProperty(Constants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils
                .getTypeInfosFromTypeString(columnTypeProperty);

        /**
         * Make sure the number of column types and column names are equal.
         */
        Preconditions.checkState(columnNames.size() == columnTypes.size());
        numColumns = columnNames.size();

        /**
         * Store the mapping between the column name and the JSONPath expression for
         * accessing that column's value within the JSON object.
         */

        // Build a hashmap from column name to JSONPath expression.
        colNameJsonPathMap = new HashMap<String, JsonPath>();
        String[] propertiesSet = new String[tblProps.stringPropertyNames().size()];
        propertiesSet = tblProps.stringPropertyNames().toArray(propertiesSet);
        String currentJsonPath;
        int z = 0;
        for (String colName : columnNames) {
            LOG.debug("Iteration #" + z);
            currentJsonPath = null;
            for (int i = 0; i < propertiesSet.length; i++) {
                LOG.debug("current property=" + propertiesSet[i]);
                if (propertiesSet[i].equalsIgnoreCase(colName)) {
                    currentJsonPath = tblProps.getProperty(propertiesSet[i]);
                    break;
                }
            }

            if (currentJsonPath == null) {
                String errorMsg = ("SERDEPROPERTIES must include a property for every column. " +
                        "Missing property for column '" + colName + "'.");
                LOG.error(errorMsg);
                throw new SerDeException(errorMsg);
            }

            LOG.info("Checking JSON path=" + currentJsonPath);
            JsonPath jsonPath = JsonPath.compile(currentJsonPath);

            if (!jsonPath.isPathDefinite()) {
                throw new SerDeException("All JSON paths must point to exaclty one item. " +
                        " The following path is ambiguous: " +
                        currentJsonPath);
            }

            LOG.debug("saving json path=" + currentJsonPath);
            // @todo consider trimming the whitespace from the tokens.
            colNameJsonPathMap.put(colName, jsonPath);
            z++;
        }

        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
                columnNames.size());
        ObjectInspector oi;
        for (int c = 0; c < numColumns; c++) {
            oi = TypeInfoUtils
                    .getStandardJavaObjectInspectorFromTypeInfo(columnTypes
                            .get(c));
            columnOIs.add(oi);
        }
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
                columnNames, columnOIs);

        // Create an empty row object to be reused during deserialization
        row = Lists.newArrayListWithCapacity(numColumns);
        for (int c = 0; c < numColumns; c++) {
            row.add(null);
        }

        LOG.debug("JsonSerde initialization complete");
    }

    /**
     * Gets the ObjectInspector for a row deserialized by this SerDe
     */
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    @Override
    public SerDeStats getSerDeStats() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Deserialize a JSON Object into a row for the table
     */
    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
        LOG.debug("Deserialize row: {}", rowText);

        // Try parsing row into JSON object
        ReadContext context = jsonReader.parse(rowText.toString());

        // Loop over columns in table and set values
        JsonPath colPath;
        Object tmpValue;
        String colValue;
        Object value;
        for (int c = 0; c < numColumns; c++) {
            LOG.debug("Processing column #{}. col name={}", c, columnNames.get(c));
            colPath = colNameJsonPathMap.get(columnNames.get(c));
            TypeInfo ti = columnTypes.get(c);

            tmpValue = context.read(colPath);

            if (tmpValue == null) {
                LOG.debug("Json path either does not exist or is null at {}", columnNames.get(c));
                value = null;
            } else {
                colValue = tmpValue.toString();

                // Get type-safe JSON values
                if (ti.getTypeName().equalsIgnoreCase(Constants.DOUBLE_TYPE_NAME)) {
                    value = Double.valueOf(colValue);
                } else if (ti.getTypeName().equalsIgnoreCase(Constants.BIGINT_TYPE_NAME)) {
                    value = Long.valueOf(colValue);
                } else if (ti.getTypeName().equalsIgnoreCase(Constants.INT_TYPE_NAME)) {
                    value = Integer.valueOf(colValue);
                } else if (ti.getTypeName().equalsIgnoreCase(Constants.TINYINT_TYPE_NAME)) {
                    value = Byte.valueOf(colValue);
                } else if (ti.getTypeName().equalsIgnoreCase(Constants.FLOAT_TYPE_NAME)) {
                    value = Float.valueOf(colValue);
                } else if (ti.getTypeName().equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)) {
                    value = Boolean.valueOf(colValue);
                } else {
                    // Fall back, just get an object
                    value = colValue;
                }
            }

            row.set(c, value);
        }

        return row;
    }

    /**
     * Not sure - something to do with serialization of data
     */
    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    /**
     * Serializes a row of data into a JSON object
     *
     * @todo Implement this - sorry!
     */
    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector)
            throws SerDeException {
        LOG.info("-----------------------------");
        LOG.info("--------- serialize ---------");
        LOG.info("-----------------------------");
        LOG.info(obj.toString());
        LOG.info(objInspector.toString());

        return null;
    }
}
