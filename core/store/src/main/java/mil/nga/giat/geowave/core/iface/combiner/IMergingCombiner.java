/**
 * 
 */
package mil.nga.giat.geowave.core.iface.combiner;

import java.util.Map;

import mil.nga.giat.geowave.core.iface.field.IKey;
import mil.nga.giat.geowave.core.iface.field.IValue;

/**
 * @author viggy
 * 
 */
public interface IMergingCombiner
{

	void init(
			ISortedKeyValueIterator<IKey, IValue> source,
			Map<String, String> options,
			IIteratorEnvironment env );

}
