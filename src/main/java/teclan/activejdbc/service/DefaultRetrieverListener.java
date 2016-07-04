package teclan.activejdbc.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import teclan.activejdbc.model.DbRecord;

public abstract class DefaultRetrieverListener implements RetrieverListener {
    protected static final Logger LOGGER = LoggerFactory
            .getLogger(DefaultRetrieverListener.class);

    /**
     * @author Teclan
     * 
     *         获取到一条记录并处理,具体处理逻辑在子类具体实现
     * 
     */

    public abstract void recordRetrieved(DbRecord record);

    public abstract DbRecord getDbRecord();

}
