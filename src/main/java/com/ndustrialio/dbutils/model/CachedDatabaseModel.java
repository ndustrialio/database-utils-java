package com.ndustrialio.dbutils.model;

import com.ndustrialio.cacheutils.Cache;
import com.ndustrialio.dbutils.JDBCClient;
import org.json.JSONObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jmhunt on 8/5/16.
 */
public class CachedDatabaseModel extends DatabaseModel
{
    private Cache<String> _cache;

    private String _cacheKey;

    public CachedDatabaseModel(JDBCClient client,
                               String tableName,
                               List<DatabaseColumn> columns,
                               List<String> primaryIdentifiers,
                               List<String> primaryValues,
                               List<String> updateValues,
                               Cache<String> cache) throws SQLException
    {
        super(client, tableName, columns, primaryIdentifiers, primaryValues, updateValues);

        _cache = cache;

        if (_primaryIdentifiers.isEmpty() || _primaryValues.isEmpty())
        {
            throw new RuntimeException("com.ndustrialio.dbutils.model.CachedDatabaseModel doesn't work with nothing to cache!");
        }

        _cacheKey = "db:" + _tableName;
    }

    @Override
    public Object insert(Map<String, Object> newRecord) throws Exception
    {
        Object ret = super.insert(newRecord);

        newRecord.put(_idColumn.getName(), ret.toString());

        // Get primary identifier cache string
        String pi = getPrimaryIdentifiers(newRecord);

        // Get string to store
        String pv = getPrimaryValues(newRecord);

        // Load into cache
        _cache.put(_cacheKey, pi, pv);


        return ret;
    }

    @Override
    public Map<String, Object> select(Map<String, Object> identifiers) throws Exception
    {
        Map<String, Object> ret=null;

        // Check cache first
        String pi = getPrimaryIdentifiers(identifiers);

        String cachedResult = _cache.get(_cacheKey, pi);

        if (cachedResult != null)
        {
            // Cache hit!
            ret = new HashMap<>();

            JSONObject res = new JSONObject(cachedResult);

            for(Object key : res.keySet())
            {
                String k = (String)key;

                Object v = res.get(k);

                ret.put(k, v);
            }

        } else
        {
            // Cache miss, load from db
            ret = super.select(identifiers);

            if (ret != null)
            {
                // Get string to store
                String pv = getPrimaryValues(ret);

                // Load into cache
                _cache.put(_cacheKey, pi, pv);
            }
        }

        return ret;
    }




    private String getPrimaryValues(Map<String, Object> record)
    {
        JSONObject ret = new JSONObject();

        for (String v : _primaryValues)
        {
            ret.put(v, record.get(v));
        }

        return ret.toString();
    }

    private String getPrimaryIdentifiers(Map<String, Object> record)
    {
        JSONObject ret = new JSONObject();

        for (String i : _primaryIdentifiers)
        {
            ret.put(i, record.get(i));

        }

        return ret.toString();

    }
}
