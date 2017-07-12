package com.ndustrialio.dbutils.statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jmhunt on 5/31/16.
 */
public class Insert extends Statement
{
    protected Map<String, String> _castTypes;

    public Insert(String tableName)
    {
        super(tableName);

        _verb = "INSERT INTO";


        _castTypes = new HashMap<>();
    }


    @Override
    public Statement verb(String v)
    {
        return this;
    }

    @Override
    public Insert columns(List<String> columns)
    {
        super.columns(columns);

        return this;
    }

    public Insert cast(String column, String castType)
    {
        _castTypes.put(column, castType);

        return this;
    }

    @Override
    public String toString()
    {
        List<String> queryChunks = new ArrayList<>();

        queryChunks.add(_verb);

        queryChunks.add(_table);

        queryChunks.add("(" + stringJoin(",", _columns) + ")");

        queryChunks.add("VALUES");

        List<String> wildCards = new ArrayList<>();

        for (String c : _columns)
        {
            String castType = _castTypes.get(c);

            if (castType != null)
            {
                wildCards.add("CAST (? AS "+ castType + ")");
            } else
            {
                wildCards.add("?");
            }
        }


        List<String> insertRows = new ArrayList<>();

        //
        insertRows.add("(" + stringJoin(",", wildCards) + ")");

        // Add any additional insert rows
        if (_arguments.size() > 1)
        {
            for(int i = 1; i < _arguments.size(); i++)
            {
                insertRows.add("(" + stringJoin(",", wildCards) + ")");
            }
        }


        queryChunks.add(stringJoin(",", insertRows));

        return stringJoin(" ", queryChunks);

    }
}
