package com.ndustrialio.dbutils.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jmhunt on 4/9/16.
 */
public abstract class Statement
{

    public static class JoinCondition
    {
        public String col1, col2;

        public JoinCondition(String col1, String col2)
        {
            this.col1 = col1;
            this.col2 = col2;
        }
    }

    protected String _verb;
    protected String _table;
    protected List<String> _columns;
    protected List<String>_joins;
    protected Where _where;
    protected String _order;
    protected Integer _limit;

    protected List<List<Object>> _arguments;


    public Statement(String tableName)
    {
        _table = tableName;
        _columns = new ArrayList<>();
        _joins = new ArrayList<>();

        _arguments = new ArrayList<>();

        _where = null;
        _order = null;
        _limit = null;
    }


    public Statement verb(String v)
    {
        _verb = v;

        return this;
    }

    public String verb()
    {
        return _verb;
    }

    public List<String> columns()
    {
        return _columns;
    }

    public Statement columns(List<String> columns, String wildcard)
    {
        for(String c : columns)
        {
            _columns.add(c + '=' + wildcard);
        }

        return this;
    }

    public Statement columns(String column, String wildcard)
    {
        _columns.add(column + "=" + wildcard);

        return this;
    }

    public Statement columns(List<String> columns)
    {
        _columns.addAll(columns);

        return this;
    }

    public Statement columns(String column)
    {
        _columns.add(column);

        return this;
    }

    public Statement join(String joinTable, List<JoinCondition> joinConditions)
    {
        return join(_table, joinTable, joinConditions);

    }

    public Statement join(String joinFrom, String joinTable, List<JoinCondition> joinConditions)
    {
        List<String> conditions = new ArrayList<>();

        for (JoinCondition c : joinConditions)
        {
            conditions.add(joinFrom + "." + c.col1 + " = " + joinTable + "." + c.col2);

        }

        _joins.add("JOIN " + joinTable + " ON " + stringJoin(" AND ", conditions));

        return this;

    }

    // Limit setter/getter
    public int limit()
    {
        return _limit;
    }

    public Statement limit(int l)
    {
        _limit = l;

        return this;
    }

    /**
     * Retrieve WHERE clause for statement
     * @return where clause
     */
    public Where where()
    {
        return _where;
    }

    /**
     * Set WHERE clause for statement
     * @param w where clause to set
     * @return current com.ndustrialio.dbutils.statement.Query object
     */
    public Statement where(Where w)
    {
        _where = w;

        return this;
    }

    /**
     * Retreive ORDER BY clause
     * @return current ordering
     */
    public String order()
    {
        return _order;
    }

    /**
     * Set statement ordering
     * @param column com.ndustrialio.dbutils.model to order on
     * @param desc order descending if true, ascending otherwise
     * @return current com.ndustrialio.dbutils.statement.Query object
     */
    public Statement order(String column, boolean desc)
    {
        if (desc)
        {
            _order = "ORDER BY " + column + "DESC";
        } else
        {
            _order = "ORDER BY " + column;
        }

        return this;
    }

    public Statement order(String column)
    {
        // Default ascending
        return order(column, false);
    }


    public static String stringJoin(String joiner, List<String> strings)
    {
        StringBuilder sb = new StringBuilder();

        for (String s : strings)
        {
            sb.append(s);
            sb.append(joiner);
        }

        // Remove trailing joiner
        sb.setLength(sb.length() - joiner.length());


        return sb.toString();
    }

    /**
     * Get argument list by index
     * @param index index of argument list to get
     * @return argument list at index, or null if none.
     */
    public List<Object> arguments(int index)
    {
        return ((index < _arguments.size()) && (index >= 0)) ? _arguments.get(index) : null;
    }


    /**
     * Add an argument to the 0th list, without clearing
     * @param argument argument to add
     * @return current com.ndustrialio.dbutils.statement.Query object
     */
    public Statement addArgument(Object argument)
    {
        if (_arguments.isEmpty())
        {
            _arguments.add(new ArrayList<>());
        }

        List<Object> args = _arguments.get(0);

        args.add(argument);

        return this;
    }


    /**
     * Add arguments to the 0th list, clearing beforehand
     * @param arguments arguments to add
     * @return current com.ndustrialio.dbutils.statement.Query object
     */
    public Statement arguments(Object... arguments)
    {
        _arguments.clear();

        List<Object> args = new ArrayList<>();
        _arguments.add(args);

        for(Object o : arguments)
        {
            args.add(o);
        }

        return this;
    }

    /**
     * Clear arguments
     * @return current com.ndustrialio.dbutils.statement.Query object
     */
    public Statement clearArguments()
    {
        _arguments.clear();

        return this;
    }

    /**
     * Add an argument list, without clearing.
     * @param arguments argument list to add
     * @return the current com.ndustrialio.dbutils.statement.Query object
     */
    public Statement addArguments(Object... arguments)
    {
        List<Object> args = Arrays.asList(arguments);
        _arguments.add(args);

        return this;
    }


    /**
     * Populate a JDBC prepared statement with statement arguments
     * @param statement JDBC PreparedStatement to populate
     * @throws SQLException
     */
    public void setArguments(PreparedStatement statement) throws SQLException
    {
        int i = 1;

        for (List<Object> arguments : _arguments)
        {
            for(Object arg : arguments)
            {
                if(arg == null)
                {
                    statement.setNull(i, Types.NULL);
                } else
                {
                    statement.setObject(i, arg);
                }

                i++;
            }
        }

    }
}
