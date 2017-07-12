package com.ndustrialio.dbutils.statement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmhunt on 1/22/17.
 */
public class CreateIndex extends Statement
{
    protected boolean _unique = true;

    public CreateIndex(String tableName)
    {
        super(tableName);
    }

    public CreateIndex unique(boolean unique)
    {
        _unique = unique;

        return this;
    }

    @Override
    public String toString()
    {
        List<String> queryChunks = new ArrayList<>();

        // CREATE (UNIQUE) INDEX IF NOT EXISTS
        queryChunks.add("CREATE");

        if (_unique)
        {
            queryChunks.add("UNIQUE");
        }

        queryChunks.add("INDEX IF NOT EXISTS");

        // column1_column2_columnN_uidx
        // Construct index name
        String indexName = _table + "_" + stringJoin("_", _columns);
        if (_unique)
        {
            indexName+="_uidx";
        }

        queryChunks.add(indexName);

        // ON (table name)
        queryChunks.add("ON");
        queryChunks.add(_table);

        //(column1, column2, columnN)
        queryChunks.add("("+stringJoin(",", _columns)+")");


        return stringJoin(" ", queryChunks);

    }
}
