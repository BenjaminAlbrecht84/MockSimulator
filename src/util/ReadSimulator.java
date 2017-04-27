package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ReadSimulator {

	public void run(File file, int numOfReads, String srcDir) {

		String NANOSIM = srcDir + "/NanoSim-master/src/simulator.py";
		String TRAINING_R73 = srcDir + "/NanoSim-master/ecoli_R7.3_training/ecoli";
		String TRAINING_R9 = srcDir + "/NanoSim-master/ecoli_R92D_training/ecoli";

		try {

			File refFile = file;
			if (file.getName().endsWith(".gz")) {
				String exe = "gunzip " + file.getAbsolutePath();
				executingCommand(exe);
				refFile = new File(file.getParentFile().getAbsolutePath() + File.separatorChar + file.getName().replaceAll(".fna.gz", ".fna"));
			}

			for (int i = 0; i < 2; i++) {

				String training = i == 0 ? TRAINING_R73 : TRAINING_R9;
				String chemistry = i == 0 ? "R73" : "R9";

				String out = refFile.getAbsolutePath().replaceAll(".fna", "_simulated_" + chemistry);
				String exe = "python " + NANOSIM + " linear -r " + refFile.getAbsolutePath() + " -c " + training + " -n " + numOfReads + " -o " + out;
				executingCommand(exe);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private int executingCommand(String command) {
		try {

//			System.out.println("Executing " + command);
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(command);

			// checking error messages
			StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "ERROR");

			// checking error messages
			StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "OUTPUT");

			errorGobbler.start();
//			outputGobbler.start();
			int exitVal = proc.waitFor();

			return exitVal;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return 1;
	}

	class StreamGobbler extends Thread {
		InputStream is;
		String type;

		StreamGobbler(InputStream is, String type) {
			this.is = is;
			this.type = type;
		}

		public void run() {
			try {
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				String line = null;
				while ((line = br.readLine()) != null)
					System.out.println(type + ">" + line);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}

}
