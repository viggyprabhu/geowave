/**
 * 
 */
package mil.nga.giat.geowave.core.iface.store;

import java.util.Iterator;

/**
 * @author viggy
 * 
 */
public interface Converter<InputType, ConvertedType>
{
	public Iterator<ConvertedType> convert(
			InputType entry );
}
