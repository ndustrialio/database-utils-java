package com.ndustrialio.dbutils;

import com.datastax.driver.core.*;
import com.ndustrialio.contxt.BaseConfiguredComponent;

import java.util.Map;

public class CassandraUtility extends BaseConfiguredComponent
{
    private static final int CORE_CONNECTIONS = 5, MAX_CONNECTIONS = 50;

    // Static CLUSTER and SESSION
	private static Cluster CLUSTER = null;
	private static Session SESSION = null;

	private static Session getSession(String host)
	{
        if (SESSION == null)
        {
            PoolingOptions poolingOptions = new PoolingOptions();

            poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, MAX_CONNECTIONS);
            poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, CORE_CONNECTIONS);

            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, MAX_CONNECTIONS);
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, CORE_CONNECTIONS);

            CLUSTER = Cluster.builder()
                    .withPoolingOptions(poolingOptions)
                    .addContactPoint(host)
                    .build();

            SESSION = CLUSTER.connect();

            return SESSION;

        } else
        {
            return SESSION;
        }
    }

    // Old connect function.. kept for reference
    //	private void connectToCassandraCluster() {
//		cluster = Cluster.builder()
//					.addContactPoint(_cassandraHost)
//					.build();
//		Metadata metadata = cluster.getMetadata();
//		LOG.info("Connected to cluster: " + metadata.getClusterName());
//		for (Host host : metadata.getAllHosts()) {
//			LOG.info("Datacenter: " + host.getDatacenter() + " Host: " + host.getAddress() + " Rack: " + host.getRack());
//		}
//		session = cluster.connect();
//	}

    private Session _session;


	private String _keySpace;
	
	public CassandraUtility(Map conf)
	{
		this.setConf(conf);
		
		String host = (String) this.getConfigurationValue("cassandra_host");

		try
		{
			_keySpace = (String) this.getConfigurationValue("cassandra_keyspace");


		} catch (RuntimeException e) {}

		_session = CassandraUtility.getSession(host);


	}



	public String getKeyspace()
	{
		return _keySpace;
	}

	
	public PreparedStatement prepareStatement(String query) {
		PreparedStatement statement = _session.prepare(query);
		statement.setConsistencyLevel(ConsistencyLevel.TWO);
		return statement;
	}

	public void execute(BoundStatement bs)
	{
		_session.execute(bs);
	}
	
	public ResultSet executeBatch(BatchStatement bs) {
		return _session.execute(bs);
	}
	
	public ResultSet executeSelect(BoundStatement bs) {
		return _session.execute(bs);
	}
	
	public ResultSet executeSelect(String cql)
	{
		return _session.execute(cql);
	}
	
	public ResultSet executeSelect(Statement statement)
	{
		return _session.execute(statement);
	}
	
}
