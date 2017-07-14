import com.ndustrialio.dbutils.JDBCClient;
import com.ndustrialio.dbutils.PostgresClient;
import com.ndustrialio.dbutils.QueryResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by jmhunt on 7/13/17.
 */
public class PostgresClientTest
{

    PostgresClient _postgres;
    @Before
    public void setup() throws SQLException
    {
        Map<String, Object> conf = new HashMap<String, Object>()
        {
            {
                put("postgres_host", "localhost");
                put("postgres_port", "5432");
                put("postgres_user", "test");
                put("postgres_pass", "test");
                put("postgres_database", "test");


            }

        };

        _postgres = new PostgresClient(conf, JDBCClient.CONNECTION_TYPE.SIMPLE, false);

        try (QueryResponse response = _postgres.executeStatement("CREATE TABLE IF NOT EXISTS test (one varchar, two varchar, three varchar)"))
        {
            response.connection.commit();
        }

        try (QueryResponse response = _postgres.executeStatement("TRUNCATE TABLE test"))
        {
            response.connection.commit();
        }

    }

    @Test
    public void runTest() throws SQLException
    {
        _postgres.executeStatement("INSERT INTO test (one, two, three) VALUES(?, ?, ?)",
                Arrays.asList("One", "Two", "Three")).commit();
        _postgres.executeStatement("INSERT INTO test (one, two, three) VALUES(?, ?, ?)",
                Arrays.asList("One", "Four", "Five")).commit();


        try (QueryResponse response = _postgres.executeStatement("SELECT two from test where one=?", Collections.singletonList("One")))
        {
            ResultSet rs = response.getResults();


            List<String> results = new ArrayList<>();

            while(rs.next())
            {
                results.add(rs.getString("two"));
            }


            Assert.assertEquals(results.get(0), "Two");
            Assert.assertEquals(results.get(1), "Four");

        }



    }
}
