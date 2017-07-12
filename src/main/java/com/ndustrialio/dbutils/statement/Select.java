package com.ndustrialio.dbutils.statement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmhunt on 4/17/16.
 */
public class Select extends Statement
{
    public Select(String tableName)
    {
        super(tableName);

        _verb = "SELECT";
    }

    @Override
    public Statement verb(String v)
    {
        // Verb can't be changed
        return this;
    }

    @Override
    public String toString()
    {
        List<String> queryChunks = new ArrayList<>();

        queryChunks.add(_verb);

        if (!_columns.isEmpty())
        {
            queryChunks.add(stringJoin(", ", _columns));
        } else
        {
            queryChunks.add("*");
        }

        queryChunks.add("FROM " + _table);

        if (!_joins.isEmpty())
        {
            queryChunks.add(stringJoin(" ", _joins));
        }

        if(_where != null)
        {
            queryChunks.add(_where.toString());
        }

        if (_limit != null)
        {
            queryChunks.add("LIMIT " + _limit.toString());
        }

        if (_order != null)
        {
            queryChunks.add(_order);
        }



        return stringJoin(" ", queryChunks);
    }

}
