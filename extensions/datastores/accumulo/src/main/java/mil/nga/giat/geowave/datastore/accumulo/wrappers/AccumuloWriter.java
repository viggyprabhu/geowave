/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.wrappers;

import mil.nga.giat.geowave.core.iface.store.CoreWriter;
import mil.nga.giat.geowave.core.iface.store.IMutation;

import org.apache.accumulo.core.client.BatchWriter;

/**
 * @author viggy
 * 
 */
public class AccumuloWriter implements
		CoreWriter
{

	private BatchWriter m_writer;

	public AccumuloWriter(
			BatchWriter writer ) {
		m_writer = writer;
	}

	@Override
	public void write(
			Iterable<IMutation> mutations ) {
		// TODO #238 Auto-generated method stub

	}

	@Override
	public void write(
			IMutation mutation ) {
		// TODO #238 Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO #238 Auto-generated method stub

	}

}
