package com.ndustrialio.dbutils;

import org.postgresql.ds.PGPoolingDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by jmhunt on 12/5/16.
 */
public class PostgresClient extends JDBCClient
{


    private static PGPoolingDataSource POOLED_SOURCE = null;



    public static PGPoolingDataSource getSharedPool(String host, int port,
                                                    String db, String user, String pass)
    {
        // Shared connection pool for this JVM instance (if running in Storm.. I think)
        if (POOLED_SOURCE == null)
        {
            POOLED_SOURCE = new PGPoolingDataSource();

            POOLED_SOURCE.setServerName(host);
            POOLED_SOURCE.setPortNumber(port);
            POOLED_SOURCE.setDatabaseName(db);
            POOLED_SOURCE.setUser(user);
            POOLED_SOURCE.setPassword(pass);

            POOLED_SOURCE.setInitialConnections(INITIAL_CONNETIONS);
            POOLED_SOURCE.setMaxConnections(MAX_CONNECTIONS);
        }

        return POOLED_SOURCE;
    }

    public PostgresClient(Map conf)
    {
        super(conf);

    }

    public PostgresClient(Map conf, CONNECTION_TYPE connection_type, boolean autoCommit)
    {
        super(conf, connection_type, autoCommit);
    }


    public PostgresClient(Map conf, CONNECTION_TYPE connection_type)
    {
        super(conf, connection_type);
    }


    @Override
    protected DataSource initDataSource()
    {
        String host = (String) this.getConfigurationValue("postgres_host");
        String db = (String) this.getConfigurationValue("postgres_database");
        int port = Integer.parseInt((String)this.getConfigurationValue("postgres_port"));
        String user = (String) this.getConfigurationValue("postgres_user");
        String pass = (String) this.getConfigurationValue("postgres_pass");


        if (_connectionType == CONNECTION_TYPE.SIMPLE)
        {
            // Initialize simple
            PGSimpleDataSource  source = new PGSimpleDataSource();
            source.setServerName(host);
            source.setPortNumber(port);
            source.setDatabaseName(db);
            source.setUser(user);
            source.setPassword(pass);

            return source;


        } else
        {
            // Get shared pooled source
            return PostgresClient.getSharedPool(host, port, db, user, pass);

        }


    }

    @Override
    public boolean checkSQLException(SQLException e)
    {
        String state = e.getSQLState();

        return state.startsWith("08");

    }

}
