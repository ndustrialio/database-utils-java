package com.ndustrialio.dbutils.statement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmhunt on 4/13/16.
 */
public class Update extends Statement
{
    public Update(String tableName)
    {
        super(tableName);

        _verb = "UPDATE";
    }

    @Override
    public Update verb(String v)
    {
        // Verb can't be changed
        return this;
    }



    @Override
    public String toString()
    {
        List<String> queryChunks = new ArrayList<>();

        queryChunks.add(_verb);

        queryChunks.add(_table);

        queryChunks.add("SET");

        queryChunks.add(stringJoin(", ", _columns));

        if(_where != null)
        {
            queryChunks.add(_where.toString());
        }

        return stringJoin(" ", queryChunks);

    }
}
