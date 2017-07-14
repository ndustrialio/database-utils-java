import com.ndustrialio.dbutils.PostgresClient;
import com.ndustrialio.dbutils.QueryResponse;
import com.ndustrialio.dbutils.statement.Insert;
import com.ndustrialio.dbutils.statement.Select;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by jmhunt on 7/14/17.
 */
public class InsertTest
{
    PostgresClient _postgres;

    @Before
    public void setup() throws SQLException
    {
        _postgres = DatabaseTesting.getLocalPostgres(true);

        _postgres.executeStatement("CREATE TABLE IF NOT EXISTS insert_test (id serial, string_column varchar)");

        _postgres.executeStatement("TRUNCATE TABLE insert_test");
    }

    @Test
    public void runTest() throws SQLException
    {
        // Perform insert and test RETURNING function
        try(QueryResponse response = _postgres.executeStatement(
                new Insert("insert_test")
                        .returning("id")
                        .columns("string_column")
                        .addArgument("test value")))
        {
            ResultSet rs = response.getResults();

            // Should have one result here
            Assert.assertTrue(rs.next());

            System.out.println(rs.getString("id"));
        }

        // Test that insert succeeded
        try (QueryResponse response = _postgres.executeStatement(new Select("insert_test").columns("string_column")))
        {
            ResultSet rs = response.getResults();

            while(rs.next())
            {
                Assert.assertEquals("test value", rs.getString("string_column"));
            }

        }

    }
}
