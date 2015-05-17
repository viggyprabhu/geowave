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
		IMergingVisibilityCombiner {

	/* (non-Javadoc)
	 * @see mil.nga.giat.geowave.core.iface.combiner.IMergingVisibilityCombiner#transformRange(mil.nga.giat.geowave.core.iface.combiner.ISortedKeyValueIterator, mil.nga.giat.geowave.core.iface.field.IKVBuffer)
	 */
	@Override
	public void transformRange(ISortedKeyValueIterator<IKey, IValue> input,
			IKVBuffer output) {
		new MergingVisibilityCombiner().transformRange(input, output);

	}

	/* (non-Javadoc)
	 * @see mil.nga.giat.geowave.core.iface.combiner.IMergingVisibilityCombiner#init(mil.nga.giat.geowave.core.iface.combiner.ISortedKeyValueIterator, java.util.Map, mil.nga.giat.geowave.core.iface.combiner.IIteratorEnvironment)
	 */
	@Override
	public void init(ISortedKeyValueIterator<IKey, IValue> source,
			Map<String, String> options, IIteratorEnvironment env) {
		// TODO Auto-generated method stub

	}

}
