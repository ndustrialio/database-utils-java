package com.ndustrialio.dbutils.statement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmhunt on 4/10/16.
 */
public class Where
{
    protected List<String> _wheres;

    public Where(String firstClause)
    {
        _wheres = new ArrayList<>();

        _wheres.add(firstClause);

    }

    public Where(String firstClaue, String wildcard)
    {
        _wheres = new ArrayList<>();

        _wheres.add(firstClaue + "=" + wildcard);
    }

    public Where(List<String> clauses)
    {
        _wheres = new ArrayList<>();

        _wheres.add(clauses.get(0));

        // Assume these are AND
        for(int i = 1; i < clauses.size(); i++)
        {
            _wheres.add("AND " + clauses.get(i));
        }
    }

    public Where(List<String> clauses, String wildcard)
    {
        _wheres = new ArrayList<>();

        _wheres.add(clauses.get(0) + "=" + wildcard);

        // Assume these are AND
        for(int i = 1; i < clauses.size(); i++)
        {
            _wheres.add("AND " + clauses.get(i) + "=" + wildcard);
        }
    }

    public Where AND(String clause)
    {
        _wheres.add("AND " + clause);

        return this;
    }

    public Where OR(String clause)
    {
        _wheres.add("OR " + clause);

        return this;
    }

    @Override
    public String toString()
    {

        return "WHERE " + Statement.stringJoin(" ", _wheres);
    }
}
