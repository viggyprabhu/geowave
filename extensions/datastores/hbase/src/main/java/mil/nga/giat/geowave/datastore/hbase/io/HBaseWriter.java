/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.io;

import java.io.IOException;

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

	public HBaseWriter(
			Table table ) {
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

	public void close() {
	}

}
