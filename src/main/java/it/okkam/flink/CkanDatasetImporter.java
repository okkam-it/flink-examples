package it.okkam.flink;

import it.okkam.flink.ckan.CkanRestInputFormat;
import it.okkam.flink.ckan.CkanRestInputFormat.CkanRestInputFormatBuilder;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;

public class CkanDatasetImporter {
	private static String catalogUrl = "http://dati.mit.gov.it/catalog/";
	private static String resourceId = "f5d1d2bc-7ea1-4dc4-af96-884b6d82bcdb";
	private static String[] dsFields = new String[] { //
			"data_scadenza", //
			"categoria_patente", //
			"data_abilitazione_a", //
			"data_rilascio", //
			"punti_patente", //
			"regione_residenza", //
			"provincia_residenza", //
			"stato_estero_primo_rilascio", //
			"abilitato_a", //
			"sesso", //
			"comune_residenza", //
			"anno_nascita", //
			"_id", //
			"id", //
			"stato_estero_nascita"//
	};

	/**
	 * Main class.
	 * 
	 * @param args the arguments
	 * @throws Exception if any exception occurs
	 */
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		CkanRestInputFormat ckanInputformat = new CkanRestInputFormatBuilder(catalogUrl, resourceId, dsFields).finish();
		DataSet<Row> rows = env.createInput(ckanInputformat);
		rows.print();
	}

}
