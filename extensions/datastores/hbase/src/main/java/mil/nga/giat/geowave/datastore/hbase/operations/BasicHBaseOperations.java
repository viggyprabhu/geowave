/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;

import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

/**
 * @author viggy
 * 
 */
public class BasicHBaseOperations
{

	private final static Logger LOGGER = Logger.getLogger(BasicHBaseOperations.class);
	private static final String HBASE_CONFIGURATION_TIMEOUT = "timeout";
	private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	private static final String DEFAULT_TABLE_NAMESPACE = "";
	private static final String DEFAULT_COLUMN_FAMILY = "GridPoint";
	private final Connection conn;
	private String tableNamespace;

	public BasicHBaseOperations(String zookeeperInstances, String geowaveNamespace)
			throws IOException {
		Configuration hConf = HBaseConfiguration.create();
		hConf.set(
				HBASE_CONFIGURATION_ZOOKEEPER_QUORUM,
				zookeeperInstances);
		hConf.setInt(
				HBASE_CONFIGURATION_TIMEOUT,
				120000);

		conn = ConnectionFactory.createConnection(hConf);
		tableNamespace = geowaveNamespace;
	}
	
	public BasicHBaseOperations(String zookeeperInstances) throws IOException {
		this(zookeeperInstances, DEFAULT_TABLE_NAMESPACE);
	}

	public HBaseWriter createWriter(
			String tableName )
					throws IOException {
		return createWriter(tableName, true);
	}

	private TableName getTableName(
			String tableName ) {
		return TableName.valueOf(tableName);
	}

	public HBaseWriter createWriter(
			final String tableName,
			final boolean createTable ){
		TableName table2 = getTableName(getQualifiedTableName(tableName));
		Table table=null;
		try {
			if (createTable && !conn.getAdmin().isTableAvailable(table2)) {
				HTableDescriptor desc = new HTableDescriptor(table2);
				desc.addFamily(new HColumnDescriptor(DEFAULT_COLUMN_FAMILY));
				conn.getAdmin().createTable(desc);
			}
			table = conn.getTable(table2);
		}
		catch (IOException e) {
			LOGGER.error(
					"Unable to create table '" + tableName + "'",
					e);
		}
		return new HBaseWriter(table);
	}
	
	private String getQualifiedTableName(
			final String unqualifiedTableName ) {
		return HBaseUtils.getQualifiedTableName(
				tableNamespace,
				unqualifiedTableName);
	}

}
