package mil.nga.giat.geowave.webservices.rest.data;

public class GeowaveRESTPublisher
{
	private final String restURL;
	private final String gsuser;
	private final String gspass;
	
	public GeowaveRESTPublisher(String restURL, String username,
			String password) {
		this.restURL = HttpUtils.decurtSlash(restURL);
		this.gsuser = username;
		this.gspass = password;
	}
	
	public boolean datastoreExist(String workspace, String datastore) {
		String sUrl = restURL + "/rest/workspaces/" + workspace
				+ "/datastores/" + datastore;
		String result = HttpUtils.get(sUrl, gsuser, gspass);
		return result.length() > 0;
	}
	
	public boolean createDatastore(String workspace,
			DatastoreEncoder encoder) {
		String sUrl = restURL + "/rest/workspaces/" + workspace
				+ "/datastores";
		String result = HttpUtils.post(sUrl, encoder.toString(), gsuser, gspass);
		return result != null;
	}

	public boolean layerExist(String layer) {
		String sUrl = restURL + "/rest/layers/" + layer;
		String result = HttpUtils.get(sUrl, gsuser, gspass);
		return result.length() > 0;
	}

	public boolean publishLayer(String workspace, String dataStore, FeatureTypeEncoder encoder ) {
		String sUrl = restURL + "/rest/workspaces/" + workspace + "/datastores/" + dataStore + "/featuretypes";
		String result = HttpUtils.post(sUrl, encoder.toString(), gsuser, gspass);
		return result != null;
	}
}
