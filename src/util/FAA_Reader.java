package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

public class FAA_Reader {

	public static ArrayList<SparseString> read(File faaFile) {

		ArrayList<SparseString> proteinIDs = new ArrayList<SparseString>();
		try {

			BufferedReader buf;
			try {
				buf = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(faaFile))));
			} catch (ZipException e) {
				buf = new BufferedReader(new FileReader(faaFile));
			}

			String line, id = "";
			boolean readSequence = false;
			StringBuilder seq = new StringBuilder("");
			while ((line = buf.readLine()) != null) {

				if (line.startsWith(">")) {
					if (seq.length() != 0 && !id.isEmpty())
						proteinIDs.add(new SparseString(id));
					seq = new StringBuilder("");
					// id = line.substring(1).split(" ")[0];
					id = line.substring(1);
					readSequence = true;
				} else if (readSequence) {
					seq.append(line);
				}

			}
			if (seq.length() != 0 && !id.isEmpty())
				proteinIDs.add(new SparseString(id));
			buf.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return proteinIDs;

	}

}
