package teclan.activejdbc.service;

public enum DataType {

    INTEGER(0), LONG(1), DOUBLE(2), FLOAT(3), NUMBER(4), BOOLEAN(5), STRING(
            6), DATETIME(7), CLOB(8), BLOB(9), COMMON_BINARY(10);

    public int value;

    private DataType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

}
