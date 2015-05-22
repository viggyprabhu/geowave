/**
 * 
 */
package mil.nga.giat.geowave.core.iface.field;

/**
 * @author viggy Interface to give access to ColumnSet in Accumulo-Core
 */
public interface IColumnSet
{

	boolean contains(
			IKey topKey );

}
