package teclan.activejdbc.service;

import teclan.activejdbc.model.DbRecord;

public interface RetrieverListener {

    public void recordRetrieved(DbRecord record);

    public DbRecord getDbRecord();

}
