package teclan.activejdbc.model;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

import org.javalite.common.Convert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import teclan.activejdbc.service.DataType;
import teclan.activejdbc.utils.JsonBuilder;
import teclan.activejdbc.utils.JsonParser;

public class DbField {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbField.class);

    private static final JsonBuilder JSON_BUILDER = new JsonBuilder();
    private static final JsonParser  JSON_PARSER  = new JsonParser();

    public String   key;
    public byte[]   value;
    public DataType dataType;

    private byte[] data;

    public DbField() {
    }

    public DbField(String key, String value) {
        this(key, value, DataType.STRING);
    }

    public DbField(String key, Object value, DataType dataType) {
        this.key = key;
        this.dataType = dataType;

        Object valueToConvert = value;
        if (DataType.DATETIME.equals(dataType)) {
            if (value instanceof java.util.Date) {
                valueToConvert = ((java.util.Date) value).getTime();
            } else if (value instanceof java.sql.Time) {
                valueToConvert = ((java.sql.Time) value).getTime();
            } else if (value instanceof java.sql.Timestamp) {
                valueToConvert = ((java.sql.Timestamp) value).getTime();
            } else if (value instanceof oracle.sql.TIMESTAMP) {
                try {
                    valueToConvert = ((oracle.sql.TIMESTAMP) value)
                            .timestampValue().getTime();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }

        // for BIT ,not kingbase
        if (DataType.INTEGER.equals(dataType)) {
            if (value.equals(true)) {
                valueToConvert = 1;
            } else if (value.equals(false)) {
                valueToConvert = 0;
            }
        }

        // for kingbase
        if (DataType.BOOLEAN.equals(dataType)) {
            if (value.equals("1")) {
                valueToConvert = true;
            } else {
                valueToConvert = false;
            }
        }

        this.value = (valueToConvert == null ? null
                : Convert.toBytes(valueToConvert));
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        if (value == null) {
            return null;
        }

        if (getDataType().equals(DataType.BLOB)) {
            return "<BLOB>...";
        } else if (getDataType().equals(DataType.CLOB)) {
            return "<CLOB>...";
        } else if (getDataType().equals(DataType.DATETIME)) {
            return new String(value);
        } else {
            return new String(value);
        }
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isBlobField() {
        return getDataType().equals(DataType.BLOB);
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return this.data;
    }

    public String toJson() {
        return JSON_BUILDER.build(this);
    }

    @Override
    public String toString() {
        return String.format("%s:%s", getKey(), getValue());
    }

    public static DbField parseFromJson(String json) {
        return JSON_PARSER.parseWithoutRoot(json, DbField.class);
    }

    public Object getRealValue() {
        if (value == null) {
            return null;
        }

        if (getDataType().equals(DataType.BLOB)) {
            return value;
        } else if (getDataType().equals(DataType.DATETIME)) {
            return getDateTimeValue();
        } else if (getDataType().equals(DataType.INTEGER)) {
            return getIntegerValue();
        } else if (getDataType().equals(DataType.LONG)) {
            return getLongValue();
        } else if (getDataType().equals(DataType.DOUBLE)
                || getDataType().equals(DataType.NUMBER)) {
            return getDoubleValue();
        } else if (getDataType().equals(DataType.FLOAT)) {
            return getFloatValue();
        } else if (getDataType().equals(DataType.BOOLEAN)) {
            return getBooleanValue();
        } else if (getDataType().equals(DataType.STRING)) {
            return getStringValue();
        } else if (getDataType().equals(DataType.CLOB)) {
            return new String(value);
        }

        return null;
    }

    private String getStringValue() {
        return getValue();
    }

    private Boolean getBooleanValue() {
        return Convert.toBoolean(getValue().trim());
    }

    private Float getFloatValue() {
        return Convert.toFloat(getValue().trim());
    }

    private Double getDoubleValue() {
        return Convert.toDouble(getValue().trim());
    }

    private Long getLongValue() {
        return Convert.toLong(getValue().trim());
    }

    private Integer getIntegerValue() {
        return Convert.toInteger(getValue().trim());
    }

    private Timestamp getDateTimeValue() {
        return Convert.toTimestamp(Long.valueOf(getValue()));
    }

    public void setData(Map<Integer, byte[]> dataMap) {
        int size = 0;
        for (byte[] data : dataMap.values()) {
            size += data.length;
        }

        this.data = new byte[size];

        for (int i = 0; i < dataMap.size(); i++) {
            System.arraycopy(dataMap.get(i), 0, this.data,
                    i * dataMap.get(0).length, dataMap.get(i).length);
        }

        this.value = this.data;
    }

}
