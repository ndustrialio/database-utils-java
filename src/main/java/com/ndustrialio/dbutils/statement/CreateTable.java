package com.ndustrialio.dbutils.statement;

import com.ndustrialio.dbutils.model.DatabaseColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmhunt on 1/21/17.
 */
public class CreateTable extends Statement
{
    protected boolean _unlogged = false;

    public CreateTable(String tableName)
    {
        super(tableName);
    }

    public CreateTable unLogged(boolean unlogged)
    {
        _unlogged = unlogged;

        return this;
    }

    public CreateTable columnDefinitions(List<DatabaseColumn> columns)
    {
        for(DatabaseColumn column : columns)
        {
            super.columns(column.toString());
        }

        return this;
    }


    @Override
    public String toString()
    {
        List<String> queryChunks = new ArrayList<>();

        queryChunks.add("CREATE");

        if (_unlogged)
        {
            queryChunks.add("UNLOGGED");
        }

        queryChunks.add("TABLE IF NOT EXISTS");
        queryChunks.add(_table);
        queryChunks.add("("+stringJoin(",", _columns)+")");


        return stringJoin(" ", queryChunks);

    }
}
