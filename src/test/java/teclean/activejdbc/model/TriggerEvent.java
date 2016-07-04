package teclean.activejdbc.model;

import org.javalite.activejdbc.annotations.Table;

import teclan.activejdbc.model.AbstractModel;

@Table("EVENTS_TABLE")
public class TriggerEvent extends AbstractModel {

    @Override
    public String getConfigTableName() {
        return "EVENTS_TABLE";
    }

}
