/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce.client;

import java.util.ArrayList;

import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;
import mil.nga.giat.geowave.core.store.data.IteratorScopeEntity;

/**
 * @author viggy
 *
 */
public class CoreIteratorConfig {

	public CoreIteratorConfig(IIteratorSetting iteratorSettings,
			IteratorScopeEntity[] scopes) {
		this.iteratorSettings = iteratorSettings;
		this.scopes = scopes;
	}
	
	public CoreIteratorConfig(IIteratorSetting iteratorSettings,
			IteratorScopeEntity iteratorScopeEntity) {
		this.iteratorSettings = iteratorSettings;
		ArrayList<IteratorScopeEntity> iIteratorScopeEntity = new ArrayList<IteratorScopeEntity>();
		iIteratorScopeEntity.add(iteratorScopeEntity);
		this.scopes = (IteratorScopeEntity[])iIteratorScopeEntity.toArray(); 
	}

	private final IIteratorSetting iteratorSettings;
	private final IteratorScopeEntity[] scopes;


	public IteratorScopeEntity[] getScopes() {
		return scopes;
	}

	public IIteratorSetting getIteratorSettings() {
		return iteratorSettings;
	}

	public String mergeOption(
			final String optionKey,
			final String currentValue,
			final String nextValue ) {
		return nextValue;
	}

}
