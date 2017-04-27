package io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class FileReader_Simple {

	public static String run(File file) {
		try {
			BufferedReader buf = new BufferedReader(new FileReader(file));
			StringBuilder build = new StringBuilder();
			String l;
			while ((l = buf.readLine()) != null)
				build.append(l);
			buf.close();
			return build.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
