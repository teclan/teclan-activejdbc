package teclan.activejdbc.service;

import teclan.activejdbc.model.DbRecord;
import teclan.activejdbc.service.DefaultRetrieverListener;

public class MysqlRetriverListener extends DefaultRetrieverListener {

    private DbRecord dbRecord;

    @Override
    public void recordRetrieved(DbRecord record) {

        this.dbRecord = record;

        LOGGER.info(record.getDetailString());

    }

    public DbRecord getDbRecord() {
        return dbRecord;
    }

    public void setDbRecord(DbRecord dbRecord) {
        this.dbRecord = dbRecord;
    }

}
