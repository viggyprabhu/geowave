/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.iface.store.CoreBatchDeleter;

import org.apache.accumulo.core.client.BatchDeleter;

/**
 * @author viggy
 *
 */
public class AccumuloBatchDeleter implements CoreBatchDeleter {

	private BatchDeleter m_batchDeleter;

	public AccumuloBatchDeleter(BatchDeleter batchDeleter) {
		m_batchDeleter = batchDeleter;
	}

}
