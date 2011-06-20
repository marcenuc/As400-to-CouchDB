package it.nuccioservizi.tailor.as400;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

public class CouchDb {
	private static final String						DESIGN_DOC_ID						= "_design/boutique";
	private static final SimpleDateFormat	FORMATO_DATA_DOCUMENTO	= new SimpleDateFormat("yyyyMMdd");

	public static CouchDbConnector get(final AppProperties properties) {
		final HttpClient http = new StdHttpClient.Builder().host(properties.get(Property.COUCHDB_HOST))
				.port(Integer.valueOf(properties.get(Property.COUCHDB_PORT)))
				.username(properties.get(Property.COUCHDB_USERNAME))
				.password(properties.get(Property.COUCHDB_PASSWORD))
				.socketTimeout(30000)
				.build();
		return new StdCouchDbInstance(http).createConnector(properties.get(Property.COUCHDB_DB), false);
	}

	public static ObjectNode getLatest(final CouchDbConnector db, final String... baseIds) {
		final String[] startKey = Arrays.copyOf(baseIds, baseIds.length + 1);
		startKey[baseIds.length] = "\ufff0";
		final ViewResult resp = db.queryView(new ViewQuery().designDocId(DESIGN_DOC_ID)
				.viewName("ids")
				.includeDocs(true)
				.descending(true)
				.limit(1)
				.startKey(startKey)
				.endKey(baseIds));

		ObjectNode doc = null;
		for (final Row row : resp.getRows()) {
			doc = (ObjectNode) row.getDocAsNode();
		}
		if (doc != null) {
			return doc;
		}
		throw new DocumentNotFoundException(baseIds.toString());
	}

	public static ObjectNode newVersionedDoc(final CouchDbConnector db, final String docId) {
		final ObjectNode newDoc = JsonNodeFactory.instance.objectNode();

		newDoc.put("_id", docId);
		try {
			final ObjectNode doc = db.get(ObjectNode.class, docId);
			newDoc.put("_rev", doc.get("_rev"));
		} catch (final DocumentNotFoundException ex) {
			// Nessuna versione precedente.
		}

		return newDoc;
	}

	public static String toDateString(final Date date) {
		return FORMATO_DATA_DOCUMENTO.format(date);
	}

}