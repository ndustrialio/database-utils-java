import com.ndustrialio.dbutils.JDBCClient;
import com.ndustrialio.dbutils.PostgresClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmhunt on 7/14/17.
 */
public class DatabaseTesting
{
    public static PostgresClient getLocalPostgres(boolean autoCommit)
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

        return new PostgresClient(conf, JDBCClient.CONNECTION_TYPE.SIMPLE, autoCommit);
    }
}
