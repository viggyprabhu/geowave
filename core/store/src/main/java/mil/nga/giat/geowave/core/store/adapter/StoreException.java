/**
 * 
 */
package mil.nga.giat.geowave.core.store.adapter;

/**
 * @author viggy
 *
 */
public class StoreException extends Exception {

	private static final long serialVersionUID = 1L;

	  /**
	   * @param why
	   *          is the reason for the error being thrown
	   */
	  public StoreException(final String why) {
	    super(why);
	  }

	  /**
	   * @param cause
	   *          is the exception that this exception wraps
	   */
	  public StoreException(final Throwable cause) {
	    super(cause);
	  }

	  /**
	   * @param why
	   *          is the reason for the error being thrown
	   * @param cause
	   *          is the exception that this exception wraps
	   */
	  public StoreException(final String why, final Throwable cause) {
	    super(why, cause);
	  }
}
