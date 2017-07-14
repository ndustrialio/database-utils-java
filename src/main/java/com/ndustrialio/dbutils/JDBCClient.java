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

        Connection conn = _dataSource.getConnection();

        conn.setAutoCommit(_autoCommit);

        PreparedStatement statement = preparer.prepareStatement(conn);

        if (statement.execute())
        {
            return new QueryResponse(conn, statement, statement.getResultSet());
        } else
        {
            return new QueryResponse(conn, statement, statement.getGeneratedKeys());
        }

    }

    /**
     * Executes a statement
     *
     * @param statement  Statement object
     * @return Resposne from dbutils
     * @throws SQLException
     */
    public QueryResponse executeStatement(final Statement statement) throws SQLException
    {

        return executeImpl((conn) ->
        {

            PreparedStatement preparedStatement = conn.prepareStatement(statement.toString());

            statement.setArguments(preparedStatement);

            return preparedStatement;
        });
    }

    /**
     * Executes a statement
     *
     * @param statement String statement, probably unsafe
     * @return Resposne from dbutils
     * @throws SQLException
     */
    public QueryResponse executeStatement(final String statement) throws SQLException
    {
        return executeImpl((conn) ->
        {
            return conn.prepareStatement(statement);
        });
    }

    /**
     * Executes a statement
     *
     * @param statement  String statement
     * @param arguments  Arguments to be inserted into the prepared statement
     * @return Resposne from dbutils
     * @throws SQLException
     */
    public QueryResponse executeStatement(final String statement, final List<Object> arguments) throws SQLException
    {

        return executeImpl((conn) ->
        {
            PreparedStatement ps;

            ps = conn.prepareStatement(statement);

            int i = 1;

            for (Object arg : arguments)
            {
                if (arg == null)
                {
                    ps.setNull(i, Types.NULL);
                } else
                {
                    ps.setObject(i, arg);
                }

                i++;
            }

            return ps;
        });
    }




}