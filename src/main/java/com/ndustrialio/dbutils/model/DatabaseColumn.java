package com.ndustrialio.dbutils.model;

import com.ndustrialio.dbutils.DataParseException;
import com.ndustrialio.dbutils.NullValueException;
import com.ndustrialio.dbutils.statement.Statement;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import java.sql.Timestamp;

/**
 * Created by jmhunt on 8/1/16.
 */
public class DatabaseColumn
{
    protected String _name;
    protected boolean _notNull = false;
    protected boolean _disableInsert = false;
    protected ColumnType _type;
    protected String _defaultValue = null;

    // TODO: if this works, incorporate it into base bolt code
    private static DateTimeParser[] parsers = {
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HHmm").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SS").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SS").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.S").getParser(),
            DateTimeFormat.forPattern("yyyyMMDDHHmmss").getParser(),
            DateTimeFormat.forPattern("yyyyMMDD").getParser()
    };

    //private static DateTimeFormatter _formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static DateTimeFormatter _formatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();


    public enum ColumnType
    {
        SERIAL,
        INTEGER,
        BOOLEAN,
        VARCHAR,
        TIMESTAMP,
        FLOAT
    }


    public DatabaseColumn(String name, ColumnType type)
    {
        _name = name;
        _type = type;
    }

    public DatabaseColumn(String name, ColumnType type, String defaultValue)
    {
        this(name, type);

        _defaultValue = defaultValue;
    }

    public DatabaseColumn setNotNull()
    {
        _notNull = true;

        return this;
    }

    public DatabaseColumn disableInsert()
    {
        _disableInsert = true;

        return this;
    }

    public boolean insertDisabled()
    {
        return _disableInsert;
    }


    public ColumnType getType()
    {
        return _type;
    }

    public String getName()
    {
        return _name;
    }

    public boolean isNotNull()
    {
        return _notNull;
    }

    public boolean hasDefaultValue()
    {
        return (_defaultValue != null);
    }

    protected Object checkStringValue(String value) throws Exception
    {

        switch(_type)
        {
            case SERIAL:
                return Integer.parseInt(value);

            case INTEGER:
                try
                {
                    Integer v;

                    if (!value.contains("."))
                    {
                        v = Integer.parseInt(value);
                    } else
                    {
                        v = ((Double)Double.parseDouble(value)).intValue();
                    }

                    return v;

                } catch (NumberFormatException e)
                {
                    if (_notNull)
                    {
                        // Can't tolerate a null value here, throw exception
                        throw new DataParseException("Integer: " + value + " failed to parse for com.ndustrialio.dbutils.model: " + _name);
                    } else
                    {
                        return null;
                    }
                }


            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case VARCHAR:
                return value;

            case TIMESTAMP:

                try
                {
                    DateTime v = _formatter.parseDateTime(value);

                    return new Timestamp(v.getMillis());
                } catch (IllegalArgumentException e)
                {
                    if (_notNull)
                    {
                        // Can't tolerate a null value here, throw exception
                        throw new DataParseException("Timestamp: " + value + " failed to parse for com.ndustrialio.dbutils.model: " + _name);
                    } else
                    {
                        return null;
                    }
                }

            case FLOAT:

                try
                {
                    return Double.parseDouble(value);

                } catch(NumberFormatException e)
                {
                    if (_notNull)
                    {
                        // Can't tolerate a null value here, throw exception
                        throw new DataParseException("Float: " + value + " failed to parse for com.ndustrialio.dbutils.model: " + _name);
                    } else
                    {
                         return null;
                    }
                }
            default:
                throw new IllegalArgumentException("Bad columnType: " + _type.toString());

        }
    }


    public String toString() {
        String columnString = _name + " " + _type.toString();
        if (_notNull) {
            columnString += " NOT NULL";
        }
        if (_defaultValue != null) {
            columnString += " DEFAULT " + _defaultValue;
        }

        return columnString;
    }

    protected boolean testNull(String value)
    {
        return value == null || value.equalsIgnoreCase("null");
//        if (value == null)
//        {
//            return true;
//        } else
//        {
//            return value.equalsIgnoreCase("null");
//        }
    }


    public void setQueryValue(String value, Statement query) throws Exception
    {
        if (testNull(value))
        {
            if (_defaultValue != null)
            {
                value = _defaultValue;
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

    public void setQueryValue(Object value, Statement query) throws Exception
    {
        // Handle strings specially
        if (value instanceof String)
        {
            setQueryValue((String)value, query);
            return;
        }

        if (value == null)
        {
            if (_defaultValue != null)
            {
                value = _defaultValue;
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

        query.addArgument(value);

    }


}
