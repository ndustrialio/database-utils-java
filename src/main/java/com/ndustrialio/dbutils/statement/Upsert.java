package com.ndustrialio.dbutils.statement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmhunt on 9/5/16.
 */
public class Upsert extends Insert
{
    protected List<String> _conflict, _update;

    public Upsert(String tableName)
    {
        super(tableName);

        _verb = "INSERT INTO";

        _conflict = new ArrayList<>();
        _update = new ArrayList<>();
    }

    public Upsert onConflict(List<String> columns)
    {
        for (String c : columns)
        {
            _conflict.add(c);
        }

        return this;
    }

    public Upsert onConflict(String column)
    {
        _conflict.add(column);

        return this;
    }

    public Upsert update(List<String> columns, String wildcard)
    {
        for (String c : columns)
        {
            _update.add(c + '=' + wildcard);
        }

        return this;
    }

    public Upsert update(String column, String wildcard)
    {
        _update.add(column + '=' + wildcard);

        return this;
    }


    @Override
    public String toString()
    {
        List<String> queryChunks = new ArrayList<>();

        // Get insert portion of statement
        queryChunks.add(super.toString());

        // Add conflict definition
        queryChunks.add("ON CONFLICT");
        queryChunks.add("(" + stringJoin(",", _conflict) + ")");


        queryChunks.add("DO UPDATE SET");

        queryChunks.add(stringJoin(", ", _update));

        return stringJoin(" ", queryChunks);

    }

}
