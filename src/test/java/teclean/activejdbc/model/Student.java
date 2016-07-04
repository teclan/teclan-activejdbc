package teclean.activejdbc.model;

import org.javalite.activejdbc.annotations.Table;

import teclan.activejdbc.model.AbstractModel;

@Table("student")
public class Student extends AbstractModel {

    @Override
    public String getConfigTableName() {
        return "student";
    }

}
