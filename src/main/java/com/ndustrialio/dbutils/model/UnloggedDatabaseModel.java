package com.ndustrialio.dbutils.model;

import com.ndustrialio.dbutils.statement.CreateTable;
import com.ndustrialio.dbutils.JDBCClient;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by jmhunt on 10/11/16.
 */
public class UnloggedDatabaseModel extends DatabaseModel
{

    public UnloggedDatabaseModel(JDBCClient client, String tableName, List<DatabaseColumn> columns, List<String> primaryIdentifiers, List<String> primaryValues, List<String> updateValues) throws SQLException
    {
        super(client, tableName, columns, primaryIdentifiers, primaryValues, updateValues);
    }

    @Override
    protected void createTable() throws SQLException
    {
        CreateTable createTableStatement =
                new CreateTable(_tableName)
                        .columnDefinitions(_columns)
                        .unLogged(true);


        _dbClient.executeStatement(createTableStatement);

    }

}
