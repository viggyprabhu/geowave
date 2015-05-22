/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.wrappers;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.iface.field.IKey;
import mil.nga.giat.geowave.core.iface.field.IValue;
import mil.nga.giat.geowave.core.iface.store.CoreBatchScanner;
import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

/**
 * @author viggy
 * 
 */
public class AccumuloBatchScanner implements
		CoreBatchScanner
{

	private BatchScanner m_batchScanner;

	public AccumuloBatchScanner(
			BatchScanner batchScanner ) {
		m_batchScanner = batchScanner;
	}

	public void fetchColumnFamily(
			Text col ) {
		m_batchScanner.fetchColumnFamily(col);
	}

	public void close() {
		m_batchScanner.close();
	}

	public Iterator<Entry<IKey, IValue>> iterator() {
		return AccumuloWraperUtils.convert(m_batchScanner.iterator());
	}

	public void fetchColumn(
			Text colFam,
			Text colQual ) {
		m_batchScanner.fetchColumn(
				colFam,
				colQual);
	}

	public void setRanges(
			Collection<Range> ranges ) {
		m_batchScanner.setRanges(ranges);
	}

	@Override
	public void addScanIterator(
			IIteratorSetting iteratorSettings ) {
		addScanIterator(AccumuloWraperUtils.getIteratorSetting(iteratorSettings));
	}

	public void addScanIterator(
			IteratorSetting cfg ) {
		m_batchScanner.addScanIterator(cfg);

	}

}
