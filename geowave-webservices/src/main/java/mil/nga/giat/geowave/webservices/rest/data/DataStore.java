package mil.nga.giat.geowave.webservices.rest.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class DataStore
{

	public DataStore() {
		super();
		workspace = new HashMap<String, String>();
		connectionParameters = new HashMap<String, String>();
	}
	
	public DataStore(JSONObject json) throws JSONException {
		this();
		JSONObject dataStore = json.getJSONObject(DATASTORE);
		
		name = dataStore.getString(NAME);
		type = dataStore.getString(TYPE);
		enabled = dataStore.getBoolean(ENABLED);
		
		JSONObject ws = dataStore.getJSONObject(WORKSPACE);
		Iterator<?> ws_keys = ws.keys();
		while(ws_keys.hasNext()) {
			String key = ws_keys.next().toString();
			String value = ws.getString(key);
			workspace.put(key, value);
		}
		
		JSONArray cp = dataStore.getJSONArray(CONNECTION_PARAMETERS);
		for(int index = 0; index < cp.length(); index++) {
			JSONArray entry = (JSONArray) cp.getJSONObject(index).opt(ENTRY);
			for(int index2 = 0; index2 < entry.length(); index2++) {
				connectionParameters.put(entry.getJSONObject(index2).getString(KEY),
						entry.getJSONObject(index2).getString(VALUE));
			}
		}

		_default = dataStore.getBoolean(DEFAULT);
		featureTypes = dataStore.getString(FEATURE_TYPES);
	}
	
	public String toJSONString() throws JSONException {
		JSONObject json = new JSONObject();
		
		JSONObject dataStore = new JSONObject();
		dataStore.put(NAME, name);
		dataStore.put(TYPE, type);
		dataStore.put(ENABLED, enabled);
		
		JSONObject ws = new JSONObject();
		for (String key : workspace.keySet()) {
			ws.put(key, workspace.get(key));
		}
		dataStore.put(WORKSPACE, ws);
		
		JSONArray cp = new JSONArray();
		JSONArray entry = new JSONArray();
		for (String key : connectionParameters.keySet()) {
			entry.put(new JSONObject().put(KEY, key)
					.put(VALUE, connectionParameters.get(key)));
		}
		cp.put(new JSONObject().put(ENTRY,entry));
		dataStore.put(CONNECTION_PARAMETERS, cp);
		
		dataStore.put(DEFAULT, _default);
		dataStore.put(FEATURE_TYPES, featureTypes);
		
		json.put(DATASTORE, dataStore);
		return json.toString();
	}
	
	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(
			String type ) {
		this.type = type;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(
			boolean enabled ) {
		this.enabled = enabled;
	}

	public Map<String, String> getWorkspace() {
		return workspace;
	}

	public void setWorkspace(
			Map<String, String> workspace ) {
		this.workspace = workspace;
	}

	public Map<String, String> getConnectionParameters() {
		return connectionParameters;
	}

	public void setConnectionParameters(
			Map<String, String> connectionParameters ) {
		this.connectionParameters = connectionParameters;
	}

	public boolean is_default() {
		return _default;
	}

	public void set_default(
			boolean _default ) {
		this._default = _default;
	}

	public String getFeatureTypes() {
		return featureTypes;
	}

	public void setFeatureTypes(
			String featureTypes ) {
		this.featureTypes = featureTypes;
	}

	private static final String DATASTORE = "dataStore";
	private static final String NAME = "name";
	private static final String TYPE = "type";
	private static final String ENABLED = "enabled";
	private static final String WORKSPACE = "workspace";
	private static final String CONNECTION_PARAMETERS = "connectionParameters";
	private static final String ENTRY = "entry";
	private static final String KEY = "@key";
	private static final String VALUE = "$";
	private static final String DEFAULT = "_default";
	private static final String FEATURE_TYPES = "featureTypes";

	private String name;
	private String type;
	private boolean enabled;
	private Map<String, String> workspace;
	private Map<String, String> connectionParameters;
	private boolean _default;
	private String featureTypes;
}
