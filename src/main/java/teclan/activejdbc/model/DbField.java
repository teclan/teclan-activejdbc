package teclan.activejdbc.model;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import teclan.activejdbc.service.DataType;
import teclan.utils.JsonBuilder;
import teclan.utils.JsonParser;

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

        if (value == null) {
            this.value = null;
            return;
        }
        this.value = Convert.toBytes(value, "UTF-8");
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        if (value == null) {
            return null;
        }

        try {
            if (getDataType().equals(DataType.BLOB)) {
                return "<BLOB>...";
            } else if (getDataType().equals(DataType.CLOB)) {
                return "<CLOB>...";
            } else if (getDataType().equals(DataType.DATETIME)) {
                return new String(value, "UTF-8");
            } else {
                return new String(value, "UTF-8");
            }
        } catch (Exception e) {
            LOGGER.debug(e.getMessage(), e);
            return null;
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
            try {
                return new String(value, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                LOGGER.debug(e.getMessage(), e);
                return null;
            }
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
