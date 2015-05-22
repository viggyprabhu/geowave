/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo;

import java.util.Map;

import mil.nga.giat.geowave.core.iface.combiner.IIteratorEnvironment;
import mil.nga.giat.geowave.core.iface.combiner.IMergingVisibilityCombiner;
import mil.nga.giat.geowave.core.iface.combiner.ISortedKeyValueIterator;
import mil.nga.giat.geowave.core.iface.field.IKVBuffer;
import mil.nga.giat.geowave.core.iface.field.IKey;
import mil.nga.giat.geowave.core.iface.field.IValue;

/**
 * @author viggy
 * 
 */
public class AccumuloMergingVisibilityCombiner implements
		IMergingVisibilityCombiner
{

	@Override
	public void transformRange(
			ISortedKeyValueIterator<IKey, IValue> input,
			IKVBuffer output ) {
		// TODO #238 Auto-generated method stub

	}

	@Override
	public void init(
			ISortedKeyValueIterator<IKey, IValue> source,
			Map<String, String> options,
			IIteratorEnvironment env ) {
		// TODO #238 Auto-generated method stub

	}

}
