/**
 * 
 */
package mil.nga.giat.geowave.core.iface.store;

import java.util.List;

import mil.nga.giat.geowave.core.iface.field.IColumn;
import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;

/**
 * @author viggy
 * 
 */
public interface IStoreCombiner
{

	void setColumns(
			IIteratorSetting iteratorSettings,
			List<IColumn> columns );

}
