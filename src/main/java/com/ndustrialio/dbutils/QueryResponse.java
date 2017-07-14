package com.ndustrialio.dbutils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by jmhunt on 12/8/16.
 */
public class QueryResponse implements AutoCloseable
{
    public Connection connection = null;
    public Statement statement = null;
    public ResultSet results = null;

    public QueryResponse(Connection conn, Statement statement, ResultSet results)
    {
        this.connection = conn;
        this.statement = statement;
        this.results = results;
    }

    public ResultSet getResults()
    {
        return results;
    }

    public void commit() throws SQLException
    {
        this.connection.commit();
    }

    @Override
    public void close() throws SQLException
    {
        if (results != null)
        {
            results.close();

        }

        if (statement != null)
        {
            statement.close();
        }

        connection.close();
    }
}
