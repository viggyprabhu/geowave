package mil.nga.giat.geowave.webservices.rest.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Layer
{

	public Layer() {
		super();
		
		namespace = new HashMap<String, String>();
		keywords = new ArrayList<Map<String, String>>();
		metadata = new ArrayList<Map<String, Map<String, String>>>();
		store = new HashMap<String, String>();
		attributes = new LinkedHashMap<String, List<Map<String, Object>>>();		
		_native = new BoundingBox(); 
		latLon = new BoundingBox();
	}
	
	public Layer(JSONObject json) throws JSONException {
		this();
		
		JSONObject feature = json.getJSONObject(FEATURETYPE);
		
		name = feature.getString(NAME);
		nativeName = feature.getString(NATIVE_NAME);
		
		for (Iterator<?> iter = feature.getJSONObject(NAMESPACE).keys();
				iter.hasNext(); ) {
			String key = iter.next().toString();
			String value = feature.getJSONObject(NAMESPACE).getString(key);
			namespace.put(key, value);
		}
		
		title = feature.getString(TITLE);
		description = feature.getString(DESCRIPTION);
		
		for(int index = 0; index < feature.getJSONArray(KEYWORDS).length(); index++) {
			Map<String, String> map = new HashMap<String, String>();
			for (Iterator<?> iter = feature.getJSONArray(KEYWORDS).getJSONObject(index).keys();
					iter.hasNext(); ) {
				String key = iter.next().toString();
				String value = feature.getJSONArray(KEYWORDS).getJSONObject(index).getString(key);
				map.put(key, value);
			}		
			keywords.add(map);
		}

		nativeCRS = feature.getString(NATIVE_CRS);
		srs = feature.getString(SRS);
		
		_native = new BoundingBox(feature.getJSONObject(NATIVE_BOUNDING_BOX)); 
		latLon = new BoundingBox(feature.getJSONObject(LAT_LON_BOX));
		
		projectionPolicy = feature.getString(PROJECTION_POLICY);
		enabled = feature.getBoolean(ENABLED);
		
		for(int index = 0; index < feature.getJSONArray(METADATA).length(); index++) {
			Map<String, Map<String, String>> outMap = new LinkedHashMap<String, Map<String, String>>();
			for (Iterator<?> iter = feature.getJSONArray(METADATA).getJSONObject(index).keys();
					iter.hasNext(); ) {
				Map<String, String> inMap = new LinkedHashMap<String, String>();
				String key =iter.next().toString();
				for (Iterator<?> iIter = feature.getJSONArray(METADATA).getJSONObject(index).getJSONObject(key).keys();
						iIter.hasNext(); ) {
					String iKey = iIter.next().toString();
					String value = feature.getJSONArray(METADATA).getJSONObject(index).getJSONObject(key).getString(iKey);
					inMap.put(iKey, value);
				}
				outMap.put(key, inMap);
			}
			metadata.add(outMap);
		}
		
		for(Iterator<?> iter = feature.getJSONObject(STORE).keys(); iter.hasNext(); ) {
			String key = iter.next().toString();
			store.put(key, feature.getJSONObject(STORE).getString(key));
		}
		
		maxFeatures = feature.getInt(MAX_FEATURES);
		numDecimals = feature.getInt(NUM_DECIMALS);
		
		for (Iterator<?> iter = feature.getJSONObject(ATTRIBUTES).keys(); iter.hasNext(); ) {
			String key = iter.next().toString();
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			
			for (int index = 0;
					index < feature.getJSONObject(ATTRIBUTES).getJSONArray(key).length();
					index++) {
				Map<String, Object> map = new LinkedHashMap<String, Object>();
				
				for (Iterator<?> iIter = 
						feature.getJSONObject(ATTRIBUTES).getJSONArray(key).getJSONObject(index).keys();
						iIter.hasNext(); ) {
					String iKey = iIter.next().toString();
					Object value = feature.getJSONObject(ATTRIBUTES)
							.getJSONArray(key).getJSONObject(index).get(iKey);
					map.put(iKey, value);					
				}
				
				list.add(map);
			}
			attributes.put(key, list);
		}	
	}
	
	public String toJSONString() throws JSONException {
		JSONObject json = new JSONObject();
		
		JSONObject feature = new JSONObject();
		
		feature.put(NAME, name);
		feature.put(NATIVE_NAME, nativeName);
		
		JSONObject ns = new JSONObject();
		for (String key : namespace.keySet()) {
			ns.put(key, namespace.get(key));
		}
		feature.put(NAMESPACE, ns);
		
		feature.put(NAMESPACE, namespace);
		
		feature.put(TITLE, title);
		feature.put(DESCRIPTION, description);
		
		JSONArray kw = new JSONArray();
		for (Map<String, String> map : keywords) {
			for(String key : map.keySet()) {
				JSONObject obj = new JSONObject();
				obj.put(key, map.get(key));
				kw.put(obj);
			}
		}
		feature.put(KEYWORDS, kw);
		
		feature.put(NATIVE_CRS, nativeCRS);
		feature.put(SRS, srs);
		
		feature.put(NATIVE_BOUNDING_BOX, _native.toJSON());
		feature.put(LAT_LON_BOX, latLon.toJSON());
		
		feature.put(PROJECTION_POLICY, projectionPolicy);
		feature.put(ENABLED, enabled);

		JSONArray md = new JSONArray();
		for (Map<String, Map<String, String>> o_map : metadata) {
			JSONObject obj = new JSONObject();
			for(String key : o_map.keySet()) {
				Map<String, String> i_map = o_map.get(key);
				JSONObject obj2 = new JSONObject();
				for (String key2 : i_map.keySet()) {
					String value = i_map.get(key2);
					obj2.put(key2, value);
				}
				obj.put(key, obj2);
			}
			md.put(obj);
		}
		feature.put(METADATA, md);
		
		JSONObject jStore = new JSONObject();
		for (String key : store.keySet()) {
			jStore.put(key, store.get(key));
		}
		feature.put(STORE, jStore);
		
		feature.put(MAX_FEATURES, maxFeatures);
		feature.put(NUM_DECIMALS, numDecimals);
		
		JSONObject jObject = new JSONObject();
		for(String key : attributes.keySet()) {
			JSONArray jArray = new JSONArray();
			for(Map<String, Object> map : attributes.get(key)) {
				JSONObject jjObject = new JSONObject();
				for(String iKey : map.keySet()) {
					Object value = map.get(iKey);
					jjObject.put(iKey, value);				
				}
				jArray.put(jjObject);
			}
			jObject.put(key, jArray);
		}
		feature.put(ATTRIBUTES, jObject);
		
		json.put(FEATURETYPE, feature);
		return json.toString();
	}
	
	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public String getNativeName() {
		return nativeName;
	}

	public void setNativeName(
			String nativeName ) {
		this.nativeName = nativeName;
	}

	public Map<String, String> getNamespace() {
		return namespace;
	}

	public void setNamespace(
			Map<String, String> namespace ) {
		this.namespace = namespace;
	}

	public List<Map<String, String>> getKeywords() {
		return keywords;
	}

	public void setKeywords(
			List<Map<String, String>> keywords ) {
		this.keywords = keywords;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(
			String title ) {
		this.title = title;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(
			String description ) {
		this.description = description;
	}

	public String getNativeCRS() {
		return nativeCRS;
	}

	public void setNativeCRS(
			String nativeCRS ) {
		this.nativeCRS = nativeCRS;
	}

	public String getSrs() {
		return srs;
	}

	public void setSrs(
			String srs ) {
		this.srs = srs;
	}

	public BoundingBox getNative() {
		return _native;
	}

	public void setNative(
			BoundingBox _native ) {
		this._native = _native;
	}

	public BoundingBox getLatLon() {
		return latLon;
	}

	public void setLatLon(
			BoundingBox latLon ) {
		this.latLon = latLon;
	}

	public String getProjectionPolicy() {
		return projectionPolicy;
	}

	public void setProjectionPolicy(
			String projectionPolicy ) {
		this.projectionPolicy = projectionPolicy;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(
			boolean enabled ) {
		this.enabled = enabled;
	}

	public List<Map<String, Map<String, String>>> getMetadata() {
		return metadata;
	}

	public void setMetadata(
			List<Map<String, Map<String, String>>> metadata ) {
		this.metadata = metadata;
	}

	public Map<String, String> getStore() {
		return store;
	}

	public void setStore(
			Map<String, String> store ) {
		this.store = store;
	}

	public int getMaxFeatures() {
		return maxFeatures;
	}

	public void setMaxFeatures(
			int maxFeatures ) {
		this.maxFeatures = maxFeatures;
	}

	public int getNumDecimals() {
		return numDecimals;
	}

	public void setNumDecimals(
			int numDecimals ) {
		this.numDecimals = numDecimals;
	}

	public Map<String, List<Map<String, Object>>> getAttributes() {
		return attributes;
	}

	public void setAttributes(
			Map<String, List<Map<String, Object>>> attributes ) {
		this.attributes = attributes;
	}

	private static final String FEATURETYPE = "featureType";
	private static final String NAME = "name";
	private static final String NATIVE_NAME = "nativeName";
	private static final String NAMESPACE = "namespace";
	private static final String TITLE = "title";
	private static final String DESCRIPTION = "description";
	private static final String KEYWORDS = "keywords";
	private static final String NATIVE_CRS = "nativeCRS";
	private static final String SRS = "srs";
	private static final String NATIVE_BOUNDING_BOX = "nativeBoundingBox";
	private static final String LAT_LON_BOX = "latLonBoundingBox";
	private static final String PROJECTION_POLICY = "projectionPolicy";
	private static final String ENABLED = "enabled";
	private static final String METADATA = "metadata";
	private static final String STORE = "store";
	private static final String MAX_FEATURES = "maxFeatures";
	private static final String NUM_DECIMALS = "numDecimals";
	private static final String ATTRIBUTES = "attributes";
//	public static final String ATTRIBUTE = "attribute";

	
	private String name;
	private String nativeName;
	private Map<String, String> namespace;
	private List<Map<String, String>> keywords;
	private String title;
	private String description;
	private String nativeCRS;
	private String srs;
	private BoundingBox _native;
	private BoundingBox latLon;
	private String projectionPolicy;
	private boolean enabled;
	private List<Map<String, Map<String, String>>> metadata;
	private Map<String, String> store;
	private int maxFeatures;
	private int numDecimals;
	private Map<String, List<Map<String, Object>>> attributes;
}
