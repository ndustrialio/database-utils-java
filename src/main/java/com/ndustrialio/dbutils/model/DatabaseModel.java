package com.ndustrialio.dbutils.model;

import com.ndustrialio.dbutils.*;
import com.ndustrialio.dbutils.statement.*;
import org.joda.time.DateTime;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jmhunt on 5/31/16.
 */
public class DatabaseModel
{

    protected Statement _insertStatement=null,
                    _selectStatement=null,
                    _updateStatement=null,
                    _upsertStatement=null;

    protected String _username, _password, _url;

    protected String _tableName;

    protected Map<String, DatabaseColumn> _columnMap;

    protected List<String> _primaryIdentifiers, _primaryValues, _updateValues, _insertValues;

    protected IDColumn _idColumn;

    protected JDBCClient _dbClient;

    // Primary table definition
    protected List<DatabaseColumn> _columns;

    public DatabaseModel(JDBCClient client, String tableName, List<DatabaseColumn> columns,
                         List<String> primaryIdentifiers, List<String> primaryValues,
                         List<String> updateValues) throws SQLException

    {

        // Get dbutils connection information\
        _dbClient = client;


        // Store table definition
        _columns = new ArrayList<>();
        _columnMap = new HashMap<>();

        // These are the names of columns we will
        _insertValues = new ArrayList<>();

        for (DatabaseColumn column : columns)
        {
            _columns.add(column);

            _columnMap.put(column.getName(), column);

            // Pick out ID com.ndustrialio.dbutils.model
            if (column instanceof IDColumn)
            {
                _idColumn = (IDColumn)column;
            }

            // This com.ndustrialio.dbutils.model should be part of an insert statement if
            // it has no default value and its type is not SERIAL and
            // it's not the updated_at com.ndustrialio.dbutils.model
            // TODO: there could be other generated com.ndustrialio.dbutils.model types besides int
            if (!column.hasDefaultValue()
                    && (column.getType() != DatabaseColumn.ColumnType.SERIAL)
                    && (!column.insertDisabled()))
            {
                _insertValues.add(column.getName());
            }
        }

        if (!_columnMap.containsKey("id"))
        {
            throw new RuntimeException("com.ndustrialio.dbutils.model.DatabaseModel must be used with schemas having an 'id' com.ndustrialio.dbutils.model!");
        }

        _tableName = tableName;

        // Store important lists, handling nulls

        if (primaryIdentifiers == null)
        {
            _primaryIdentifiers = new ArrayList<>();
        } else
        {
            _primaryIdentifiers = primaryIdentifiers;
        }

        if (primaryValues == null)
        {
            // If no primary values provided, will put ID in
            _primaryValues = new ArrayList<>();
            _primaryValues.add("id");
        } else
        {
            _primaryValues = primaryValues;
        }

        if (updateValues == null)
        {
            _updateValues = new ArrayList<>();
        } else
        {
            _updateValues = updateValues;
        }

        // Construct primary SQL statements
        // SELECT
        _selectStatement = new Select(_tableName)
                .columns(_primaryValues);

        // Add WHERE clause, if necessary
        if (!_primaryIdentifiers.isEmpty())
        {
            _selectStatement.where(new Where(_primaryIdentifiers, "?"));
        }


        // INSERT
        _insertStatement = new Insert(_tableName)
                .columns(_insertValues);

        // UPDATE
        if (!_updateValues.isEmpty())
        {
            _updateStatement = new Update(_tableName)
                    .columns(_updateValues, "?");

            // Add WHERE clause, if necessary.
            if (!_primaryIdentifiers.isEmpty())
            {
                _updateStatement.where(new Where(_primaryIdentifiers, "?"));
            }
        }


        // UPSERT
        if (!(_primaryIdentifiers.isEmpty() || _updateValues.isEmpty()))
        {
            _upsertStatement = new Upsert(_tableName)
                    .onConflict(_primaryIdentifiers)
                    .update(_updateValues, "?")
                    .columns(_insertValues);
        }


        // Create table / index
        createTable();
    }


    protected void createTable() throws SQLException
    {
        CreateTable createTableStatement =
                new CreateTable(_tableName)
                    .columnDefinitions(_columns);


        _dbClient.executeStatement(createTableStatement);


    }

    public DatabaseModel createIndex() throws SQLException
    {
        if (!_primaryIdentifiers.isEmpty())
        {
            CreateIndex createIndexStatement =
                    new CreateIndex(_tableName);

            createIndexStatement.columns(_primaryIdentifiers);


            // Construct and execute CREATE INDEX statement
            _dbClient.executeStatement(createIndexStatement);
        }

        return this;
    }

    /**
     * Inserts a new record into the dbutils
     * @param newRecord Record to insert
     * @return Generated ID, or null if none
     * @throws Exception
     */
    public Object insert(Map<String, Object> newRecord) throws Exception
    {
        _insertStatement.clearArguments();


        for (String insertValue : _insertValues)
        {
            DatabaseColumn column = _columnMap.get(insertValue);

            Object value = newRecord.get(column.getName());

            column.setQueryValue(value, _insertStatement);

        }

        Object ret = null;


        try (QueryResponse result = _dbClient.executeStatement(_insertStatement))
        {
            DatabaseColumn idColumn = _columnMap.get("id");

            if (idColumn != null)
            {
                DatabaseColumn.ColumnType idType = idColumn.getType();

                ResultSet rs = result.getResults();

                while (rs.next())
                {
                    // Only support UUID(varchar) and integer types
                    switch(idType)
                    {
                        case SERIAL:
                        case INTEGER:
                            ret = rs.getInt("id");
                            break;
                        case VARCHAR:
                            ret = rs.getString("id");
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported generated key type!");
                    }
                }
            }
        }

        return ret;

    }

    /**
     * Selects a single row from the dbutils, based on the primary identifiers
     * @param identifiers Map containing primary identifier values
     * @return Map containing primary values, or null if no result
     * @throws Exception
     */
    public Map<String, Object> select(Map<String, Object> identifiers) throws Exception
    {
        Map<String, Object> ret = null;

        _selectStatement.clearArguments();

        for (String primaryIdentifier : _primaryIdentifiers)
        {
            DatabaseColumn column = _columnMap.get(primaryIdentifier);

            column.setQueryValue(identifiers.get(primaryIdentifier), _selectStatement);

        }

        try (QueryResponse result = _dbClient.executeStatement(_selectStatement))
        {
            ResultSet rs = result.getResults();

            while (rs.next())
            {
                ret = new HashMap<>();

                for (String p : _primaryValues)
                {
                    DatabaseColumn column = _columnMap.get(p);

                    switch (column.getType())
                    {
                        case INTEGER:
                        case SERIAL:
                            ret.put(p, rs.getInt(p));
                            break;
                        case VARCHAR:
                            ret.put(p, rs.getString(p));
                            break;
                        case FLOAT:
                            ret.put(p, rs.getDouble(p));
                            break;
                        case BOOLEAN:
                            ret.put(p, rs.getBoolean(p));
                            break;
                        case TIMESTAMP:
                            ret.put(p, new DateTime(rs.getTimestamp(p)));
                            break;
                        default:
                            throw new IllegalArgumentException("Unsuppported primary value com.ndustrialio.dbutils.model type on select");
                    }
                }
            }
        }

        return ret;
    }

    /**
     * Updates a row
     * @param values Map containing update values
     * @throws Exception
     */
    public void update(Map<String, Object> values) throws Exception
    {

        if (_updateStatement != null)
        {
            _updateStatement.clearArguments();

            // Set update values
            for (String updateValue : _updateValues)
            {
                DatabaseColumn column = _columnMap.get(updateValue);

                if (column.getName().equals("updated_at"))
                {
                    // Support setting now on updated at automatically, without having
                    // to include it in the update map
                    _updateStatement.addArgument(new Timestamp(DateTime.now().getMillis()));
                } else
                {
                    column.setQueryValue(values.get(updateValue), _updateStatement);
                }

            }

            for (String primaryIdentifier : _primaryIdentifiers)
            {
                DatabaseColumn column = _columnMap.get(primaryIdentifier);

                column.setQueryValue(values.get(primaryIdentifier), _updateStatement);
            }

            _dbClient.executeStatement(_updateStatement);
        }
    }


    /**
     * Upserts a row
     * @param values Data to upsert
     * @throws Exception
     */
    public void upsert(Map<String, Object> values) throws Exception
    {

        if (_upsertStatement != null)
        {
            _upsertStatement.clearArguments();

            // First set insert values
            for(String insertValue : _insertValues)
            {
                DatabaseColumn insertColumn = _columnMap.get(insertValue);

                Object value = values.get(insertColumn.getName());

                insertColumn.setQueryValue(value, _upsertStatement);

            }

            // Next set update values
            for (String updateValue : _updateValues)
            {
                DatabaseColumn column = _columnMap.get(updateValue);

                if (column.getName().equals("updated_at"))
                {
                    // Support setting now on updated at automatically, without having
                    // to include it in the update map
                    _upsertStatement.addArgument(new Timestamp(DateTime.now().getMillis()));
                } else
                {
                    column.setQueryValue(values.get(updateValue), _upsertStatement);
                }

            }

            _dbClient.executeStatement(_upsertStatement);
        }
    }

}
