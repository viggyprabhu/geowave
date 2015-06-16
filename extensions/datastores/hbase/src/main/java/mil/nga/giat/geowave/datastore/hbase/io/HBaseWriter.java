/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.io;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

/**
 * @author viggy
 * 
 */
public class HBaseWriter
{

	private final static Logger LOGGER = Logger.getLogger(HBaseWriter.class);
	private Table table;
	private Admin admin;

	public HBaseWriter(
			Admin admin, Table table ) {
		this.admin = admin;
		this.table = table;
	}

	public void write(
			Iterable<RowMutations> iterable ) {
		for (RowMutations rowMutation : iterable) {
			try {
				table.mutateRow(rowMutation);
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to insert row into the table",
						e);
			}
		}
	}

	public void close() {}

	public void write(Iterable<RowMutations> iterable, String columnFamily) {
		try {
			if(!columnFamilyExists(columnFamily)){
				admin.addColumn(table.getName(), new HColumnDescriptor(columnFamily));
			}
			write(iterable);
		} catch (IOException e) {
			LOGGER.warn(
					"Unable to add column family "+columnFamily,
					e);
		}
	}

	private boolean columnFamilyExists(String columnFamily) throws IOException {
		for(HColumnDescriptor cDesc: table.getTableDescriptor().getColumnFamilies()){
			if(cDesc.getNameAsString().matches(columnFamily))
				return true;
		}
		return false;
	}

}
