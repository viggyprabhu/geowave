/**
 * 
 */
package mil.nga.giat.geowave.core.iface.store;


/**
 * This interface is returned by AccumuloOperations and useful for general
 * purpose writing of entries. The default implementation of AccumuloOperations
 * will wrap this interface with a BatchWriter but can be overridden for other
 * mechanisms to write the data.
 */
public interface CoreWriter
{
	public void write(
			Iterable<IMutation> mutations );

	public void write(
			IMutation mutation );

	public void close();
}