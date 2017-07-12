package com.ndustrialio.dbutils.model;

/**
 * Created by jmhunt on 9/2/16.
 */
public abstract class IDColumn extends DatabaseColumn
{
    public IDColumn(ColumnType type)
    {
        super("id", type);

    }
}
