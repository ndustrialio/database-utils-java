package com.ndustrialio.dbutils.model;

import com.ndustrialio.dbutils.NullValueException;
import com.ndustrialio.dbutils.statement.Statement;
import org.joda.time.DateTime;

/**
 * Created by jmhunt on 8/1/16.
 */
public class TimestampColumn extends DatabaseColumn
{
    protected boolean _defaultNow = false;

    public TimestampColumn(String name)
    {
        super(name, ColumnType.TIMESTAMP);

    }

    public TimestampColumn setDefaultNow()
    {
        _defaultNow = true;

        return this;
    }

    public boolean isDefaultNow()
    {
        return _defaultNow;
    }

    @Override
    public boolean hasDefaultValue()
    {
        return _defaultNow;
    }

    @Override
    public void setQueryValue(String value, Statement query) throws Exception
    {
        if (testNull(value))
        {
            if (_defaultNow)
            {
                query.addArgument(new java.sql.Timestamp(DateTime.now().getMillis()));

                return;
            } else
            {
                if (_notNull)
                {
                    throw new NullValueException("Unset value for com.ndustrialio.dbutils.model: " + this._name);
                } else
                {
                    query.addArgument(null);
                    return;
                }
            }
        }

        query.addArgument(checkStringValue(value));
    }


    public String toString() {
        String columnString = _name + " " + _type.toString();
        if (_notNull) {
            columnString += " NOT NULL";
        }
        if (_defaultNow) {
            columnString += " DEFAULT now()";
        }

        return columnString;
    }

}
