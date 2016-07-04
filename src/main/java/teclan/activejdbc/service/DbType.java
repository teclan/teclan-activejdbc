package teclan.activejdbc.service;

public enum DbType {

    ORACLE("ORACLE"), DAMENG("DAMENG"), SQLSERVER2K(
            "SQLSERVER2K"), SQLSERVER2KPLUS("SQLSERVER2KPLUS"), MYSQL(
                    "MYSQL"), KINGBASE("KINGBASE"), DB2("DB2");

    private String value;

    private DbType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
