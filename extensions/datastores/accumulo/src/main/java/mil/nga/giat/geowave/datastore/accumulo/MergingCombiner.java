package mil.nga.giat.geowave.datastore.accumulo;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import mil.nga.giat.geowave.core.iface.combiner.IIteratorEnvironment;
import mil.nga.giat.geowave.core.iface.combiner.IMergingCombiner;
import mil.nga.giat.geowave.core.iface.combiner.ISortedKeyValueIterator;
import mil.nga.giat.geowave.core.iface.field.IKey;
import mil.nga.giat.geowave.core.iface.field.IValue;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class MergingCombiner extends
		Combiner implements IMergingCombiner
{
	@Override
	public Value reduce(
			final Key key,
			final Iterator<Value> iter ) {
		Mergeable currentMergeable = null;
		while (iter.hasNext()) {
			final Value val = iter.next();
			// hopefully its never the case that null stastics are stored,
			// but just in case, check
			final Mergeable mergeable = getMergeable(
					key,
					val.get());
			if (mergeable != null) {
				if (currentMergeable == null) {
					currentMergeable = mergeable;
				}
				else {
					currentMergeable.merge(mergeable);
				}
			}
		}
		if (currentMergeable != null) {
			return new Value(
					getBinary(currentMergeable));
		}
		return super.getTopValue();
	}

	protected Mergeable getMergeable(
			final Key key,
			final byte[] binary ) {
		return PersistenceUtils.fromBinary(
				binary,
				Mergeable.class);
	}

	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return PersistenceUtils.toBinary(mergeable);
	}

	@Override
	public void init(ISortedKeyValueIterator<IKey, IValue> source,
			Map<String, String> options, IIteratorEnvironment env) {
		SortedKeyValueIterator<Key, Value> modifiedSource = (SortedKeyValueIterator<Key, Value>) source;
		IteratorEnvironment modifiedEnv = (IteratorEnvironment) env;
		try {
			super.init(modifiedSource,options,modifiedEnv);
		} catch (IOException e) {
			// TODO #238 Fix the exception handling
			e.printStackTrace();
		}
	}
}
