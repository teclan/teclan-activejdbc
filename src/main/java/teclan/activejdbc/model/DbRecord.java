package teclan.activejdbc.model;

import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Joiner;

import teclan.utils.JsonBuilder;

public class DbRecord {

    private static final JsonBuilder JSON_BUILDER = new JsonBuilder();

    public String        dbName;
    public String        tableName;
    public String        action;
    public List<DbField> dbFields;
    public List<DbField> pkFields;

    private List<DbField> blobFields;

    public DbRecord() {
    }

    public DbRecord(String dbName, String tableName, String action,
            List<DbField> pkFields, List<DbField> dbFields) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.action = action;
        this.pkFields = pkFields;
        this.dbFields = dbFields;
    }

    public List<DbField> getDbFields() {
        return dbFields;
    }

    public void setDbFields(List<DbField> dbFields) {
        this.dbFields = dbFields;
    }

    public List<DbField> getPkFields() {
        return pkFields;
    }

    public void setPkFields(List<DbField> pkFields) {
        this.pkFields = pkFields;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public final String getDbName() {
        return dbName;
    }

    public final void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public boolean isDeletedRecord() {
        return getAction().equalsIgnoreCase("DELETE");
    }

    public int getFieldLength() {
        return getPkFields().size() + getDbFields().size();
    }

    public List<DbField> getBlobFields() {
        if (blobFields == null) {
            blobFields = new LinkedList<DbField>();

            for (DbField dbField : getPkFields()) {
                if (dbField.isBlobField()) {
                    blobFields.add(dbField);
                }
            }
            for (DbField dbField : getDbFields()) {
                if (dbField.isBlobField()) {
                    blobFields.add(dbField);
                }
            }
        }

        return blobFields;
    }

    public String toJson() {
        return JSON_BUILDER.build(this);
    }

    @Override
    public String toString() {
        return String.format("库名：%s；表名：%s；主键：%s", getDbName(), getTableName(),
                Joiner.on(",").join(getPkFields()));
    }

    public String getDetailString() {
        return String.format("库名：%s；表名：%s；主键域：%s;数据域:%s", getDbName(),
                getTableName(), Joiner.on(",").join(getPkFields()),
                Joiner.on(",").join(getDbFields()));
    }

}
