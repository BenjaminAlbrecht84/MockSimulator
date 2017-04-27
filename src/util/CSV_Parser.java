package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;

public class CSV_Parser {

	public static void main(String[] args) {

		File csvFile = new File("/Users/Benjamin/Desktop/genomes_proks.csv");

		try {
			BufferedReader buf = new BufferedReader(new FileReader(csvFile));
			String l;
			while ((l = buf.readLine()) != null) {
				if (!l.startsWith("#")) {
					System.out.println(l.split("\\,")[0].replaceAll("\"", ""));
				}
			}
			buf.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static HashMap<String, String> run(File csvFile) {

		HashMap<String, String> taxonToFTP = new HashMap<String, String>();
		try {
			BufferedReader buf = new BufferedReader(new FileReader(csvFile));
			String l;
			while ((l = buf.readLine()) != null) {
				if (!l.startsWith("#")) {
					String[] split = l.split(",");
					String taxon = split[0].replaceAll("\"", "");
					String ftp = split[19];
					taxonToFTP.put(taxon, ftp);
				}
			}
			buf.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return taxonToFTP;
	}

}
