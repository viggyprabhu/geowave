/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.IOException;

import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

/**
 * @author viggy
 *
 */
public class BasicHBaseOperations {

	private final static Logger LOGGER = Logger.getLogger(BasicHBaseOperations.class);
	private static final String HBASE_CONFIGURATION_TIMEOUT = "timeout";
	private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	private static final String DEFAULT_TABLE_NAMESPACE = "";
	private final Connection conn;

	public BasicHBaseOperations() throws IOException {
		Configuration hConf = HBaseConfiguration.create();
		//TODO #406 Need to get zookeeper hosts from configuration
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "localhost");
		//TODO #406 Need to get zookeeper hosts from configuration
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, "2181");
		
		hConf.setInt(HBASE_CONFIGURATION_TIMEOUT, 120000);

		conn = ConnectionFactory.createConnection(hConf);
	}
	
	public HBaseWriter createWriter(String tableName) throws IOException {
		Table table = conn.getTable(getTableName(tableName));
		return new HBaseWriter(table);
	}

	private TableName getTableName(String tableName) {
		return TableName.valueOf(tableName);
	}

}
