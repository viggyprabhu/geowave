/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.wrappers;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.iface.store.CoreBatchDeleter;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * @author viggy
 *
 */
public class AccumuloBatchDeleter implements CoreBatchDeleter {

	private BatchDeleter m_batchDeleter;

	public AccumuloBatchDeleter(BatchDeleter batchDeleter) {
		m_batchDeleter = batchDeleter;
	}

	public void setRanges(List<Range> ranges) {
		m_batchDeleter.setRanges(ranges);
		
	}

	public void fetchColumnFamily(Text col) {
		m_batchDeleter.fetchColumnFamily(col);
	}

	public void delete() throws MutationsRejectedException, TableNotFoundException {
		m_batchDeleter.delete();
	}

	public void fetchColumn(Text colFam, Text colQual) {
		m_batchDeleter.fetchColumn(colFam, colQual);
	}

	public Iterator<Entry<Key, Value>> iterator() {
		return m_batchDeleter.iterator();
	}

	public void close() {
		m_batchDeleter.close();
	}

}
