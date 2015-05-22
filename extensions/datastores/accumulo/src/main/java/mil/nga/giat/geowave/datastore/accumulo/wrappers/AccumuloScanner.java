/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.wrappers;

import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.iface.field.IKey;
import mil.nga.giat.geowave.core.iface.field.IValue;
import mil.nga.giat.geowave.core.iface.store.CoreScanner;
import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;

import org.apache.accumulo.core.client.Scanner;
import org.apache.hadoop.io.Text;

/**
 * @author viggy
 * 
 */
public class AccumuloScanner implements
		CoreScanner
{

	private Scanner m_scanner;

	public AccumuloScanner(
			Scanner scanner ) {
		m_scanner = scanner;
	}

	@Override
	public void addScanIterator(
			IIteratorSetting iteratorSettings ) {
		m_scanner.addScanIterator(AccumuloWraperUtils.getIteratorSetting(iteratorSettings));
	}

	@Override
	public void fetchColumnFamily(
			Text col ) {
		m_scanner.fetchColumnFamily(col);
	}

	@Override
	public void close() {
		m_scanner.close();
	}

	@Override
	public Iterator<Entry<IKey, IValue>> iterator() {
		return AccumuloWraperUtils.convert(m_scanner.iterator());
	}

}
