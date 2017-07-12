package com.ndustrialio.dbutils;

import com.ndustrialio.dbutils.model.IDColumn;

/**
 * Created by jmhunt on 9/2/16.
 */
public class SerialIdColumn extends IDColumn
{

    public SerialIdColumn()
    {
        super(ColumnType.SERIAL);

        _notNull = true;
        _disableInsert = true;
    }
}
