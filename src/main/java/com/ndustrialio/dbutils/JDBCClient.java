package com.ndustrialio.dbutils;

import com.ndustrialio.dbutils.statement.Statement;
import com.ndustrialio.contxt.BaseConfiguredComponent;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Created by jmhunt on 1/24/17.
 */
public abstract class JDBCClient extends BaseConfiguredComponent
{

    public static final int RECONNECT_ATTEMPTS = 5;

    public static final int INITIAL_CONNETIONS = 5;

    public static final int MAX_CONNECTIONS = 50;


    public enum CONNECTION_TYPE
    {
        SIMPLE,
        POOLED
    }

    protected CONNECTION_TYPE _connectionType;

    protected DataSource _dataSource;

    protected boolean _autoCommit;

    private interface StatementPreparer
    {
        /**
         * Produces a JDBC PreparedStatement
         *
         * @param conn Connection with which to prepare statement
         * @return Statement prepared by connection
         * @throws SQLException
         */
        PreparedStatement prepareStatement(Connection conn) throws SQLException;
    }

    /**
     * Base constructor, calls initDataSource()
     * Sets default connection type SIMPLE
     *
     * @param conf Configuration map
     */
    public JDBCClient(Map conf)
    {
        this(conf, CONNECTION_TYPE.SIMPLE);
    }

    /**
     * Base constructor, calls initDataSource()
     *
     * @param conf Configuration map
     * @param type Requested connection type
     */
    public JDBCClient(Map conf, CONNECTION_TYPE type)
    {

        this(conf, type, true);
    }

    public JDBCClient(Map conf, CONNECTION_TYPE type, boolean autoCommit)
    {
        this.setConf(conf);

        _connectionType = type;

        _dataSource = initDataSource();

        _autoCommit = autoCommit;
    }


    /**
     * Initializes data source for this JDBC client
     *
     * @return DataSource instance
     */
    protected abstract DataSource initDataSource();

    /**
     * Determines if a SQLException is recoverable
     *
     * @param e exception to check
     * @return true if recoverable, false otherwise
     */
    public abstract boolean checkSQLException(SQLException e);


    /**
     * Gets connection, executes statement, returns response
     *
     * @param preparer StatementPreparer instance
     * @return com.ndustrialio.dbutils.statement.Query response
     * @throws SQLException
     */
    private QueryResponse executeImpl(StatementPreparer preparer) throws SQLException
    {

        Connection conn = null;
        ResultSet rs = null;

        try
        {
            conn = _dataSource.getConnection();

            conn.setAutoCommit(_autoCommit);

            PreparedStatement statement = preparer.prepareStatement(conn);

            if (statement.execute())
            {
                return new QueryResponse(conn, statement, statement.getResultSet());
            } else
            {
                return new QueryResponse(conn, statement, statement.getGeneratedKeys());
            }

        } finally
        {
            // At least make sure we return the connection to the pool.
            if (conn != null)
            {
                conn.close();
            }
        }

    }

    /**
     * Executes a statement
     *
     * @param query      String statement, probably unsafe
     * @param returnKeys true to return generated keys
     * @return Resposne from dbutils
     * @throws SQLException
     */
    public QueryResponse executeStatement(final String query, final boolean returnKeys) throws SQLException
    {
        return executeImpl((conn) ->
        {
            if (returnKeys)
            {
                return conn.prepareStatement(query, java.sql.Statement.RETURN_GENERATED_KEYS);
            } else
            {
                return conn.prepareStatement(query);
            }
        });
    }

    /**
     * Executes a statement
     *
     * @param query      String statement
     * @param arguments  Arguments to be inserted into the prepared statement
     * @param returnKeys true to return generated keys
     * @return Resposne from dbutils
     * @throws SQLException
     */
    public QueryResponse executeStatement(final String query, final List<Object> arguments, final boolean returnKeys) throws SQLException
    {

        return executeImpl((conn) ->
        {

            PreparedStatement statement;

            if (returnKeys)
            {
                statement = conn.prepareStatement(query, java.sql.Statement.RETURN_GENERATED_KEYS);
            } else
            {
                statement = conn.prepareStatement(query);
            }


            int i = 1;

            for (Object arg : arguments)
            {
                if (arg == null)
                {
                    statement.setNull(i, Types.NULL);
                } else
                {
                    statement.setObject(i, arg);
                }

                i++;
            }

            return statement;
        });
    }

    /**
     * Executes a statement, returning any generated keysx`
     *
     * @param query String statement, probably unsafe
     * @return Response from dbutils
     * @throws SQLException
     */
    public QueryResponse executeStatement(final String query) throws SQLException
    {
        return executeStatement(query, true);
    }

    /**
     * Executes a statement
     *
     * @param query     String statement
     * @param arguments Arguments to be inserted into the prepared statement
     * @return Response from dbutils
     * @throws SQLException
     */
    public QueryResponse executeStatement(final String query, final List<Object> arguments) throws SQLException
    {
        return executeStatement(query, arguments, false);
    }


    /**
     * Executes a statement
     *
     * @param statement  Statement object
     * @param returnKeys true to return generated keys
     * @return Resposne from dbutils
     * @throws SQLException
     */
    public QueryResponse executeStatement(final Statement statement, final boolean returnKeys) throws SQLException
    {

        return executeImpl((conn) ->
        {

            PreparedStatement preparedStatement;

            if (returnKeys)
            {
                preparedStatement = conn.prepareStatement(statement.toString(), java.sql.Statement.RETURN_GENERATED_KEYS);
            } else
            {
                preparedStatement = conn.prepareStatement(statement.toString());
            }

            statement.setArguments(preparedStatement);

            return preparedStatement;
        });
    }

    /**
     * Executes a statement, returning any generated keys
     *
     * @param statement Statement object
     * @return Response from dbutils
     * @throws SQLException
     */
    public QueryResponse executeStatement(final Statement statement) throws SQLException
    {
        return executeStatement(statement, true);
    }


}