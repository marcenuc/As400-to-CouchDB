package it.nuccioservizi.tailor.as400;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;

public class As400Importer {

	private static final BigDecimal	CENTO														= new BigDecimal(100);

	private static final String			ID_SCALARINI										= "scalarini";
	private static final String			ID_MODELLI_E_SCALARINI					= "modelli_e_scalarini";
	private static final String			ID_AZIENDA											= "azienda/";

	private static final String[]		CLIENTI_MAGAZZINO								= { "019999", "099990" };
	private static final String			CLIENTE_DISPONIBILE							= "019998";

	private static final String			SELECT_FROM_SALMOD							= "SELECT SSTAGI AS STAGIONE, SMODEL AS MODELLO,"
																																			+ " SARTIC AS ARTICOLO, SCOLOR AS COLORE,"
																																			+ " STAGLI AS DESCRIZIONE_TAGLIA,"
																																			+ " SSCALA AS SCALARINO, SQUANT AS QTA FROM ABB_DATV3.SALMOD"
																																			+ " WHERE SCODMA='D' AND SCLIDI=2 AND SQUANT>0";

	private static final String			SELECT_FROM_ANMOD00F						= "SELECT ANSTAG AS STAGIONE, ANMODE AS MODELLO,"
																																			+ " ANDESC AS DESCRIZIONE, ANSCAL AS SCALARINO"
																																			+ " FROM ABB_DATV3.ANMOD00F WHERE ANARTI='0'";

	private static final String			SELECT_SCALARINI_FROM_TABELL01	= "SELECT T$KEY AS KEY, T$CAMP AS VALUES"
																																			+ " FROM ABB_DATV3.TABELL01 WHERE T$KEY LIKE 'SC%'";

	private static final String			SELECT_FROM_ANALIS01						= "SELECT CSTGAL AS STAGIONE, CMODAL AS MODELLO,"
																																			+ " CARTAL AS ARTICOLO, ALP1$ AS COSTO"
																																			+ " FROM ABB_DATV3.ANALIS01 WHERE TIPR LIKE 'A' AND VERLAL=1";

	private static final int				LENGTH_CODICE_STAGIONE					= 3;
	private static final int				LENGTH_CODICE_MODELLO						= 5;
	private static final int				LENGTH_CODICE_ARTICOLO					= 4;
	private static final int				LENGTH_CODICE_COLORE						= 4;
	private static final int				LENGTH_CODICE_TAGLIA						= 2;
	private static final int				LENGTH_DESCRIZIONE_TAGLIA				= 3;

	private static boolean checkCodiciSM(final String stagione, final String modello) {
		if (stagione == null || stagione.length() != LENGTH_CODICE_STAGIONE) {
			System.out.println("stagione NON VALIDA: " + stagione);
			return false;
		}
		if (modello == null || modello.length() != LENGTH_CODICE_MODELLO) {
			System.out.println("modello NON VALIDO: " + modello);
			return false;
		}
		return true;
	}

	private static boolean checkCodiciSMAC(final String stagione, final String modello, final String articolo, final String colore) {
		if (stagione == null || stagione.length() != LENGTH_CODICE_STAGIONE) {
			System.out.println("stagione NON VALIDA: " + stagione);
			return false;
		}
		if (modello == null || modello.length() != LENGTH_CODICE_MODELLO) {
			System.out.println("modello NON VALIDO: " + modello);
			return false;
		}
		if (articolo == null || articolo.length() != LENGTH_CODICE_ARTICOLO) {
			System.out.println("articolo NON VALIDO: " + articolo);
			return false;
		}
		if (colore == null || colore.length() != LENGTH_CODICE_COLORE) {
			System.out.println("colore NON VALIDO: " + colore);
			return false;
		}
		return true;
	}

	private static String getSelectFromAncl200f(final String codiceAzienda) {
		return "SELECT RASCL AS NOME, INDCL AS INDIRIZZO, LOCCL AS COMUNE, PROCL AS PROVINCIA,"
				+ " CAPCL AS CAP, NTECL AS TELEFONO, CNOTE AS NOTE, CDNAZ AS NAZIONE, CDFIS AS CODICE_FISCALE, FAXCL AS FAX"
				+ " FROM ACG_DATV3.ANCL200F WHERE CDCLI='" + codiceAzienda + "'";
	}

	private static String getSelectFromOrdet00f(final String codiceCliente) {
		return "SELECT DESTAG AS STAGIONE, DEMODE AS MODELLO, DEARTI AS ARTICOLO, DECOLO AS COLORE, DESCAD AS SCALARINO,"
				+ "   DEQT01 AS QTA0, DEQT02 AS QTA1, DEQT03 AS QTA2, DEQT04 AS QTA3, DEQT05 AS QTA4, DEQT06 AS QTA5,"
				+ "   DEQT07 AS QTA6, DEQT08 AS QTA7, DEQT09 AS QTA8, DEQT10 AS QTA9, DEQT11 AS QTA10, DEQT12 AS QTA11,"
				+ "   DESTA2 AS STATO_ARTICOLO FROM ABB_DATV3.ORDET00F WHERE (DESTA2='1' OR DESTA2='2') AND DETIPR LIKE 'A' AND DECLIE='"
				+ codiceCliente + "'";
	}

	private static String getStatoArticolo(final String statoPolmone) {
		if (statoPolmone.equals("1"))
			return "IN_PRODUZIONE";
		if (statoPolmone.equals("2"))
			return "PRONTO";
		throw new IllegalArgumentException("Valore ORDET00F.DESTA2 sconosciuto: " + statoPolmone);
	}

	public static void main(final String[] args) throws SQLException, IOException {
		final AppProperties properties;
		try {
			properties = AppProperties.load();
		} catch (final IllegalArgumentException ex) {
			System.out.println("Mancano alcuni parametri di configurazione:");
			System.out.println(ex.getMessage());
			System.out.println("Creare o modificare il file '" + AppProperties.LOCAL_PROPERTIES_FILE_NAME + "'.");
			System.out.println("In alternativa si possono definire passando parametri nel formato -Dproprietà=valore al comando.");
			System.exit(1);
			throw new IllegalStateException(ex.getMessage());
		}

		final Statement statement;
		{
			final String host = properties.get(Property.AS400_HOST);
			final String username = properties.get(Property.AS400_USERNAME);
			final String password = properties.get(Property.AS400_PASSWORD);

			if (!properties.isSet(Property.AS400_ENABLE_PORTMAPPING)) {
				// Make it work through ssh tunnel.
				final com.ibm.as400.access.AS400 as400 = new com.ibm.as400.access.AS400(host);
				as400.setServicePortsToDefault();
			}

			DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
			final Connection connection = DriverManager.getConnection("jdbc:as400://" + host, username, password);

			statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}

		final CouchDbConnector couchDb = CouchDb.get(properties);

		final String dataInventario = CouchDb.toDateString(new Date());

		/*
		 * Importazione aziende (ANCL200F).
		 */
		{
			final ViewResult idAziende = couchDb.queryView(new ViewQuery().allDocs().startKey(ID_AZIENDA).endKey(ID_AZIENDA + "\ufff0"));
			for (final Row row : idAziende.getRows()) {
				final String docId = row.getId();
				final String codiceAzienda = docId.split("/", 2)[1];
				final String query = getSelectFromAncl200f(codiceAzienda);
				System.out.println(query);

				final ResultSet ancl200f = statement.executeQuery(query);
				while (ancl200f.next()) {
					final ObjectNode azienda = couchDb.get(ObjectNode.class, docId);
					boolean aggiornaAzienda = false;
					aggiornaAzienda = setString(ancl200f, "nome", azienda) || aggiornaAzienda;
					aggiornaAzienda = setString(ancl200f, "indirizzo", azienda) || aggiornaAzienda;
					aggiornaAzienda = setString(ancl200f, "comune", azienda) || aggiornaAzienda;
					aggiornaAzienda = setString(ancl200f, "provincia", azienda) || aggiornaAzienda;
					aggiornaAzienda = setString(ancl200f, "cap", azienda) || aggiornaAzienda;
					aggiornaAzienda = setString(ancl200f, "note", azienda) || aggiornaAzienda;
					aggiornaAzienda = setString(ancl200f, "nazione", azienda) || aggiornaAzienda;

					{
						final ArrayNode contatti = JsonNodeFactory.instance.arrayNode();
						{
							final String val = ancl200f.getString("TELEFONO");
							final String telefono = val == null ? "" : val.trim();
							if (!telefono.isEmpty()) {
								contatti.add(telefono);
							}
						}
						{
							final String val = ancl200f.getString("FAX");
							final String fax = val == null ? "" : val.trim();
							if (!fax.isEmpty()) {
								contatti.add(fax.matches("[a-zA-Z]") ? fax : fax+" (fax)");
							}
						}
						{
							final JsonNode oc = azienda.get("contatti");

							if (contatti.size() > 0) {
								if (oc == null || !oc.isArray() || oc.size() != contatti.size()) {
									aggiornaAzienda = true;
								}
								else {
									for (int i = 0, n = contatti.size(); aggiornaAzienda == false && i < n; ++i) {
										aggiornaAzienda = !contatti.get(i).equals(oc.get(i)) || aggiornaAzienda;
									}
								}
								azienda.put("contatti", contatti);
							}
							else {
								aggiornaAzienda = azienda.remove("contatti") != null || aggiornaAzienda;
							}
						}
					}

					if (aggiornaAzienda) {
						System.out.println("Aggiorno " + docId);
						couchDb.update(azienda);
					}
				}
			}
		}

		/*
		 * Importazione scalarini (TABELL01).
		 */
		final ObjectNode scalarini;
		{
			final Map<String, String> righeCodici = new HashMap<String, String>();
			final Map<String, String[]> listaDescrizioni = new HashMap<String, String[]>();
			{
				final Pattern patternKeyScalarino = Pattern.compile("^SC([AB])(\\d)$");

				System.out.println(SELECT_SCALARINI_FROM_TABELL01);
				final ResultSet tabell01 = statement.executeQuery(SELECT_SCALARINI_FROM_TABELL01);
				while (tabell01.next()) {
					final String key = tabell01.getString("KEY");
					final Matcher keyMatcher = patternKeyScalarino.matcher(key);
					if (!keyMatcher.matches()) {
						System.out.println("CODICE SCALARINO IGNOTO: T$KEY=" + key);
						continue;
					}
					final String values = tabell01.getString("VALUES");
					if (values == null || values.trim().isEmpty()) {
						System.out.println("VALORI SCALARINO VUOTI: T$KEY=" + key);
						continue;
					}

					final String tipoCodice = keyMatcher.group(1);
					if (tipoCodice.equals("A")) {
						righeCodici.put(keyMatcher.group(2), values);
					}
					else if (tipoCodice.equals("B")) {
						final int maxVals = values.length() / LENGTH_DESCRIZIONE_TAGLIA;
						final List<String> ds = new ArrayList<String>(maxVals);
						for (int i = 0; i < maxVals; ++i) {
							final String val = token(values, i, LENGTH_DESCRIZIONE_TAGLIA).trim();
							if (val.isEmpty())
								break;
							ds.add(val);
						}

						listaDescrizioni.put(keyMatcher.group(2), ds.toArray(new String[0]));
					}
					else {
						throw new IllegalArgumentException("Tipo scalarino sconosciuto: " + key);
					}
				}
			}

			final ObjectNode docScalariniAs400 = JsonNodeFactory.instance.objectNode();
			final ObjectNode descrizioniAs400 = docScalariniAs400.putObject("descrizioni");
			final ObjectNode codiciAs400 = docScalariniAs400.putObject("codici");
			final ObjectNode posizioniCodiciAs400 = docScalariniAs400.putObject("posizioni_codici");
			final ObjectNode posizioneDescrizioniAs400 = docScalariniAs400.putObject("posizione_descrizioni");
			final ObjectNode posizioneCodiciAs400 = docScalariniAs400.putObject("posizione_codici");

			boolean aggiornaScalarini = false;
			final ObjectNode descrizioniCouchDb;
			final ObjectNode codiciCouchDb;
			final ObjectNode posizioniCodiciCouchDb;
			final ObjectNode posizioneDescrizioniCouchDb;
			final ObjectNode posizioneCodiciCouchDb;
			{
				ObjectNode docScalariniCouchDb = null;
				try {
					docScalariniCouchDb = couchDb.get(ObjectNode.class, ID_SCALARINI);
					docScalariniAs400.put("_id", docScalariniCouchDb.get("_id").getTextValue());
					docScalariniAs400.put("_rev", docScalariniCouchDb.get("_rev").getTextValue());
				} catch (final DocumentNotFoundException ex) {
					aggiornaScalarini = true;
				}
				descrizioniCouchDb = aggiornaScalarini ? null : (ObjectNode) docScalariniCouchDb.get("descrizioni");
				codiciCouchDb = aggiornaScalarini ? null : (ObjectNode) docScalariniCouchDb.get("codici");
				posizioniCodiciCouchDb = aggiornaScalarini ? null : (ObjectNode) docScalariniCouchDb.get("posizioni_codici");
				posizioneDescrizioniCouchDb = aggiornaScalarini ? null : (ObjectNode) docScalariniCouchDb.get("posizione_descrizioni");
				posizioneCodiciCouchDb = aggiornaScalarini ? null : (ObjectNode) docScalariniCouchDb.get("posizione_codici");
				if (!aggiornaScalarini) {
					aggiornaScalarini = descrizioniCouchDb == null || codiciCouchDb == null || posizioniCodiciCouchDb == null
							|| posizioneDescrizioniCouchDb == null || posizioneCodiciCouchDb == null;
				}
			}

			for (final String codiceScalarino : listaDescrizioni.keySet()) {
				final ObjectNode desCodCDB;
				final ObjectNode codCDB;
				final ArrayNode posCodsCDB;
				final ObjectNode posDescsCDB;
				final ObjectNode posCodisCDB;
				if (!aggiornaScalarini) {
					desCodCDB = (ObjectNode) descrizioniCouchDb.get(codiceScalarino);
					codCDB = (ObjectNode) codiciCouchDb.get(codiceScalarino);
					posCodsCDB = (ArrayNode) posizioniCodiciCouchDb.get(codiceScalarino);
					posDescsCDB = (ObjectNode) posizioneDescrizioniCouchDb.get(codiceScalarino);
					posCodisCDB = (ObjectNode) posizioneCodiciCouchDb.get(codiceScalarino);
					aggiornaScalarini = desCodCDB == null || codCDB == null || posCodsCDB == null || posDescsCDB == null
							|| posCodisCDB == null;
				}
				else {
					desCodCDB = null;
					codCDB = null;
					posCodsCDB = null;
					posDescsCDB = null;
					posCodisCDB = null;
				}

				final ObjectNode desCod = descrizioniAs400.putObject(codiceScalarino);
				final ObjectNode cod = codiciAs400.putObject(codiceScalarino);
				final ArrayNode posCods = posizioniCodiciAs400.putArray(codiceScalarino);
				final ObjectNode posDescs = posizioneDescrizioniAs400.putObject(codiceScalarino);
				final ObjectNode posCodis = posizioneCodiciAs400.putObject(codiceScalarino);

				int i = 0;
				for (final String desc : listaDescrizioni.get(codiceScalarino)) {
					final String codice = token(righeCodici.get(codiceScalarino), i, LENGTH_CODICE_TAGLIA);
					desCod.put(desc, codice);
					cod.put(codice, desc);
					posCods.add(codice);
					posDescs.put(desc, i);
					posCodis.put(codice, i);
					if (!aggiornaScalarini) {
						aggiornaScalarini = !(sameText(desCod, desCodCDB, desc) && sameText(cod, codCDB, codice)
								&& sameInt(posDescs, posDescsCDB, desc) && sameInt(posCodis, posCodisCDB, desc) //
								&& posCodsCDB.has(i) && codice.equals(posCodsCDB.get(i).getTextValue()));
					}
					++i;
				}

				if (!aggiornaScalarini) {
					aggiornaScalarini = !(desCodCDB.size() == desCod.size() && codCDB.size() == cod.size() && posCodsCDB.size() == posCods.size())
							&& posDescsCDB.size() == posDescs.size() && posCodisCDB.size() == posCodis.size();
				}
			}

			if (aggiornaScalarini || descrizioniAs400.size() != descrizioniCouchDb.size() || codiciAs400.size() != codiciCouchDb.size()
					|| posizioniCodiciAs400.size() != posizioniCodiciCouchDb.size()
					|| posizioneDescrizioniAs400.size() != posizioneDescrizioniCouchDb.size()
					|| posizioneCodiciAs400.size() != posizioneCodiciCouchDb.size()) {
				System.out.println("Aggiorno scalarini.");
				if (docScalariniAs400.has("_rev")) {
					couchDb.update(docScalariniAs400);
				}
				else {
					couchDb.create(ID_SCALARINI, docScalariniAs400);
				}
			}

			scalarini = docScalariniAs400;
		}

		/*
		 * Importazione ANMOD00F.
		 */
		final ObjectNode modelli;
		{
			final ObjectNode docModelliAs400 = JsonNodeFactory.instance.objectNode();
			final ObjectNode modelliAs400 = docModelliAs400.putObject("lista");

			boolean aggiornaModelli = false;
			final ObjectNode modelliCouchDb;
			{
				{
					ObjectNode docModelliCouchDb = null;
					try {
						docModelliCouchDb = couchDb.get(ObjectNode.class, ID_MODELLI_E_SCALARINI);
						docModelliAs400.put("_id", docModelliCouchDb.get("_id").getTextValue());
						docModelliAs400.put("_rev", docModelliCouchDb.get("_rev").getTextValue());
					} catch (final DocumentNotFoundException ex) {
						aggiornaModelli = true;
					}
					modelliCouchDb = aggiornaModelli ? null : (ObjectNode) docModelliCouchDb.get("lista");
				}

				System.out.println(SELECT_FROM_ANMOD00F);
				final ResultSet anmod00f = statement.executeQuery(SELECT_FROM_ANMOD00F);
				while (anmod00f.next()) {
					final String stagione = anmod00f.getString("STAGIONE");
					final String modello = anmod00f.getString("MODELLO");
					if (!checkCodiciSM(stagione, modello))
						continue;
					final String descrizione;
					{
						final String des = anmod00f.getString("DESCRIZIONE");
						if (des == null || des.trim().isEmpty()) {
							System.out.println("MANCA DESCRIZIONE: stagione=" + stagione + ", modello=" + modello + ".");
							continue;
						}
						descrizione = des.trim();
					}
					final String scalarino = anmod00f.getString("SCALARINO");
					if (scalarino == null || scalarino.isEmpty()) {
						System.out.println("MANCA SCALARINO: stagione=" + stagione + ", modello=" + modello + ", descrizione=" + descrizione
								+ ".");
						continue;
					}
					final String codiceSM = stagione + modello;

					if (!aggiornaModelli) {
						final ArrayNode desscalModello = (ArrayNode) modelliCouchDb.get(codiceSM);
						aggiornaModelli = desscalModello == null || !descrizione.equals(desscalModello.get(0).getTextValue())
								|| !scalarino.equals(desscalModello.get(1).toString());
					}

					final ArrayNode desscalModelloAs400 = modelliAs400.putArray(codiceSM);
					desscalModelloAs400.add(descrizione);
					desscalModelloAs400.add(Integer.parseInt(scalarino));
				}
			}

			if (aggiornaModelli || modelliCouchDb != null && modelliAs400.size() != modelliCouchDb.size()) {
				System.out.println("Aggiorno anagrafe modelli.");
				if (docModelliAs400.has("_rev")) {
					couchDb.update(docModelliAs400);
				}
				else {
					couchDb.create(ID_MODELLI_E_SCALARINI, docModelliAs400);
				}
			}
			modelli = modelliAs400;
		}

		/*
		 * Lettura listino venditori (ANALIS01).
		 */
		final Map<String, Long> listino = new HashMap<String, Long>(100000);
		{
			System.out.println(SELECT_FROM_ANALIS01);
			final ResultSet analis01 = statement.executeQuery(SELECT_FROM_ANALIS01);
			while (analis01.next()) {
				final String stagione = analis01.getString("STAGIONE");
				final String modello = analis01.getString("MODELLO");
				if (!checkCodiciSM(stagione, modello))
					continue;
				final String articolo = padArticolo(analis01.getString("ARTICOLO"));
				final Long costo = analis01.getBigDecimal("COSTO").multiply(CENTO).longValueExact();
				listino.put(stagione + modello + articolo, costo);
			}
			System.out.println(listino.size());
		}

		/*
		 * Importazione ORDET00F.
		 */
		{
			final ObjectNode posizioniCodiciScalarino = (ObjectNode) scalarini.get("posizioni_codici");

			for (final String codiceCliente : CLIENTI_MAGAZZINO) {
				final ObjectNode inventarioMagazzino = JsonNodeFactory.instance.objectNode();
				{
					inventarioMagazzino.put("causale", "INVENTARIO");
					final ArrayNode movimenti = inventarioMagazzino.putArray("movimenti");

					final String query = getSelectFromOrdet00f(codiceCliente);
					System.out.println(query);

					final ResultSet ordet00f = statement.executeQuery(query);
					while (ordet00f.next()) {
						final String stagione = ordet00f.getString("STAGIONE");
						final String modello = ordet00f.getString("MODELLO");
						final String articolo = padArticolo(ordet00f.getString("ARTICOLO"));
						final String colore = ordet00f.getString("COLORE");
						if (!checkCodiciSMAC(stagione, modello, articolo, colore))
							continue;

						final ArrayNode desscalModello = (ArrayNode) modelli.get(stagione + modello);
						if (desscalModello == null) {
							System.out.println("MODELLO NON IN ANAGRAFE: stagione=" + stagione + ", modello=" + modello + ".");
						}
						else {
							final String scalarino = ordet00f.getString("SCALARINO");
							final String scalarinoMagazzino = desscalModello.get(1).toString();
							if (!scalarino.equals(scalarinoMagazzino)) {
								System.out.println("SCALARINO IN ANAGRAFE DIVERSO: stagione=" + stagione + ", modello=" + modello
										+ ", scalarino=" + scalarinoMagazzino + ". Scalarino in anagrafe=" + scalarino + ".");
							}
							else {
								final Long costo = listino.get(stagione + modello + articolo);
								if (costo == null) {
									System.out.println("ARTICOLO NON IN LISTINO: stagione=" + stagione + ", modello=" + modello + ", articolo="
											+ articolo + ".");
								}

								final String statoArticolo = getStatoArticolo(ordet00f.getString("STATO_ARTICOLO"));
								final ArrayNode posizioneCodiciScalarino = (ArrayNode) posizioniCodiciScalarino.get(scalarino);

								for (int i = 0, n = posizioneCodiciScalarino.size(); i < n; ++i) {
									final long quantità = ordet00f.getBigDecimal("QTA" + i).longValueExact();
									if (quantità > 0) {
										final ObjectNode dettaglio = movimenti.addObject();
										dettaglio.put("codice_a_barre", stagione + modello + articolo + colore
												+ padTaglia(posizioneCodiciScalarino.get(i).getTextValue()));
										dettaglio.put("descrizione", desscalModello.get(0).getTextValue());
										dettaglio.put("quantità", quantità);
										dettaglio.put("stato_articolo", statoArticolo);
										if (costo != null) {
											dettaglio.put("costo_di_acquisto", costo);
										}
									}
								}
							}
						}
					}
				}

				boolean aggiornaInventario = false;
				final ObjectNode inventarioCouchDb;
				{
					{
						ObjectNode docInventarioCouchDb = null;
						try {
							docInventarioCouchDb = CouchDb.getLatest(couchDb, "movimenti", codiceCliente);
						} catch (final DocumentNotFoundException ex) {
							aggiornaInventario = true;
						}
						inventarioCouchDb = docInventarioCouchDb;
					}

					if (!aggiornaInventario) {
						final ArrayNode movimentiAs400 = (ArrayNode) inventarioMagazzino.get("movimenti");
						final ArrayNode movimentiCouchDb = (ArrayNode) inventarioCouchDb.get("movimenti");
						if (movimentiAs400 == null) {
							aggiornaInventario = movimentiCouchDb != null;
						}
						else if (movimentiCouchDb == null || movimentiAs400.size() != movimentiCouchDb.size()) {
							aggiornaInventario = true;
						}
						else {
							for (int i = 0, n = movimentiAs400.size(); i < n && !aggiornaInventario; ++i) {
								final ObjectNode a = (ObjectNode) movimentiAs400.get(i);
								final ObjectNode b = (ObjectNode) movimentiCouchDb.get(i);
								aggiornaInventario = !(sameText(a, b, "codice_a_barre") && sameText(a, b, "descrizione")
										&& sameLong(a, b, "costo_di_acquisto") && sameText(a, b, "stato_articolo") && sameLong(a, b, "quantità"));
							}
						}
					}
				}

				if (aggiornaInventario) {
					salvaInventario(couchDb, dataInventario, codiceCliente, inventarioMagazzino, inventarioCouchDb);
				}
			}
		}

		/*
		 * Importazione SALMOD.
		 */
		{
			final ObjectNode inventarioDisponibile = JsonNodeFactory.instance.objectNode();
			{
				inventarioDisponibile.put("causale", "INVENTARIO");
				final ArrayNode movimenti = inventarioDisponibile.putArray("movimenti");

				final ObjectNode descrizioniScalarino = (ObjectNode) scalarini.get("descrizioni");

				System.out.println(SELECT_FROM_SALMOD);
				final ResultSet salmod = statement.executeQuery(SELECT_FROM_SALMOD);
				while (salmod.next()) {
					final String stagione = salmod.getString("STAGIONE");
					final String modello = salmod.getString("MODELLO");
					final String articolo = padArticolo(salmod.getString("ARTICOLO"));
					final String colore = salmod.getString("COLORE");
					if (!checkCodiciSMAC(stagione, modello, articolo, colore))
						continue;

					final JsonNode desscalModello = modelli.get(stagione + modello);
					if (desscalModello == null) {
						System.out.println("MODELLO NON IN ANAGRAFE: stagione=" + stagione + ", modello=" + modello + ".");
					}
					else {
						final String scalarino = salmod.getString("SCALARINO");
						final String scalarinoMagazzino = desscalModello.get(1).toString();
						if (!scalarino.equals(scalarinoMagazzino)) {
							System.out.println("SCALARINO IN ANAGRAFE DIVERSO: stagione=" + stagione + ", modello=" + modello + ", scalarino="
									+ scalarinoMagazzino + ". Scalarino in anagrafe=" + scalarino + ".");
						}
						else {
							final ObjectNode descrizioniTaglie = (ObjectNode) descrizioniScalarino.get(scalarino);
							final long quantità = salmod.getBigDecimal("QTA").longValueExact();
							if (quantità > 0) {
								final ObjectNode dettaglio = movimenti.addObject();

								final String descrizioneTaglia = salmod.getString("DESCRIZIONE_TAGLIA").trim();
								dettaglio.put("codice_a_barre",
										stagione + modello + articolo + colore + padTaglia(descrizioniTaglie.get(descrizioneTaglia).getTextValue()));
								dettaglio.put("descrizione", desscalModello.get(0).getTextValue());
								dettaglio.put("quantità", quantità);
								dettaglio.put("stato_articolo", "PRONTO");

								final Long costo = listino.get(stagione + modello + articolo);
								if (costo == null) {
									System.out.println("ARTICOLO NON IN LISTINO: stagione=" + stagione + ", modello=" + modello + ", articolo="
											+ articolo + ".");
								}
								else {
									dettaglio.put("costo_di_acquisto", costo);
								}
							}
						}
					}
				}
			}

			boolean aggiornaInventario = false;
			final ObjectNode inventarioCouchDb;
			{
				{
					ObjectNode docInventarioCouchDb = null;
					try {
						docInventarioCouchDb = CouchDb.getLatest(couchDb, "movimenti", CLIENTE_DISPONIBILE);
					} catch (final DocumentNotFoundException ex) {
						aggiornaInventario = true;
					}
					inventarioCouchDb = docInventarioCouchDb;
				}

				if (!aggiornaInventario) {
					final ArrayNode movimentiAs400 = (ArrayNode) inventarioDisponibile.get("movimenti");
					final ArrayNode movimentiCouchDb = (ArrayNode) inventarioCouchDb.get("movimenti");
					if (movimentiAs400 == null) {
						aggiornaInventario = movimentiCouchDb != null;
					}
					else if (movimentiCouchDb == null || movimentiAs400.size() != movimentiCouchDb.size()) {
						aggiornaInventario = true;
					}
					else {
						for (int i = 0, n = movimentiAs400.size(); i < n && !aggiornaInventario; ++i) {
							final ObjectNode a = (ObjectNode) movimentiAs400.get(i);
							final ObjectNode b = (ObjectNode) movimentiCouchDb.get(i);
							aggiornaInventario = !(sameText(a, b, "codice_a_barre") && sameText(a, b, "descrizione")
									&& sameLong(a, b, "costo_di_acquisto") && sameText(a, b, "stato_articolo") && sameLong(a, b, "quantità"));
						}
					}
				}
			}
			if (aggiornaInventario) {
				salvaInventario(couchDb, dataInventario, CLIENTE_DISPONIBILE, inventarioDisponibile, inventarioCouchDb);
			}
		}
	}

	private static String padArticolo(final String codiceArticolo) {
		return codiceArticolo.length() == 3 ? "0" + codiceArticolo : codiceArticolo;
	}

	private static String padTaglia(final String codiceTaglia) {
		return codiceTaglia.length() == 1 ? "0" + codiceTaglia : codiceTaglia;
	}

	private static void salvaInventario(final CouchDbConnector couchDb, final String dataInventario, final String codiceCliente,
			final ObjectNode inventarioMagazzino, final ObjectNode inventarioCouchDb) {
		final String docId;
		{
			final long id;
			if (inventarioCouchDb == null) {
				id = 1;
			}
			else {
				final String[] ids = inventarioCouchDb.get("_id").getTextValue().split("/");
				if (ids[2].equals(dataInventario)) {
					id = Long.parseLong(ids[3]) + 1;
				}
				else {
					id = 1;
				}
			}
			docId = "movimenti/" + codiceCliente + "/" + dataInventario + "/" + id;
		}
		System.out.println("Aggiorno inventario " + docId);
		couchDb.create(docId, inventarioMagazzino);
	}

	private static boolean sameInt(final ObjectNode a, final ObjectNode b, final String campo) {
		final boolean hasCampo = a.has(campo);
		return hasCampo == b.has(campo) && (!hasCampo || a.get(campo).getIntValue() == b.get(campo).getIntValue());
	}

	private static boolean sameLong(final ObjectNode a, final ObjectNode b, final String campo) {
		final boolean hasCampo = a.has(campo);
		return hasCampo == b.has(campo) && (!hasCampo || a.get(campo).getLongValue() == b.get(campo).getLongValue());
	}

	private static boolean sameText(final ObjectNode a, final ObjectNode b, final String campo) {
		final boolean hasCampo = a.has(campo);
		return hasCampo == b.has(campo) && (!hasCampo || a.get(campo).getTextValue().equals(b.get(campo).getTextValue()));
	}

	private static boolean setString(final ResultSet record, final String campo, final ObjectNode obj) throws SQLException {
		final String newValue;
		{
			String v = record.getString(campo.toUpperCase());
			if (v != null) {
				v = v.trim();
			}
			newValue = v == null || v.isEmpty() ? null : v;
		}

		final JsonNode oldValue = obj.remove(campo);
		if (newValue != null) {
			obj.put(campo, newValue);
			return oldValue == null || !oldValue.isTextual() || !newValue.equals(oldValue.getTextValue());
		}
		return oldValue != null;
	}

	private static String token(final String tokens, final int tokenIndex, final int tokenLength) {
		final int startIndex = tokenLength * tokenIndex;
		return tokens.substring(startIndex, startIndex + tokenLength);
	}

}
