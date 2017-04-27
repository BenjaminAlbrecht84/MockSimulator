package startUp;

import java.io.File;

public class Main {

	public static void main(String[] args) {

		// new Runner().run();
		File srcDir = null;
		Integer cores = null;
		Integer numOfReads = null;
		Integer chunkIndex = null;
		Integer chunks = null;

		boolean verbose = false;

		for (int i = 0; i < args.length; i++) {
			String option = args[i];
			switch (option) {
			case "-s":
				try {
					srcDir = new File(args[i + 1]);
				} catch (Exception e) {
					System.err.print("ERROR: not a file " + (args[i + 1]));
				}
				i++;
				break;
			case "-p":
				try {
					cores = Integer.parseInt(args[i + 1]);
				} catch (Exception e) {
					System.err.print("ERROR: not an integer " + (args[i + 1]));
				}
				i++;
				break;
			case "-r":
				try {
					numOfReads = Integer.parseInt(args[i + 1]);
				} catch (Exception e) {
					System.err.print("ERROR: not an integer " + (args[i + 1]));
				}
				i++;
				break;
			case "-i":
				try {
					chunkIndex = Integer.parseInt(args[i + 1]);
				} catch (Exception e) {
					System.err.print("ERROR: not an integer " + (args[i + 1]));
				}
				i++;
				break;
			case "-k":
				try {
					chunks = Integer.parseInt(args[i + 1]);
				} catch (Exception e) {
					System.err.print("ERROR: not an integer " + (args[i + 1]));
				}
				i++;
				break;
			}

		}

		if (srcDir == null || cores == null || numOfReads == null || chunkIndex == null || chunks == null) {
			System.out.println("Mandatory Options: ");
			System.out.println("-s\t" + "path to source folder (containing proteins and genomes and so on...)");
			System.out.println("-p\t" + "number of assigned processors");
			System.out.println("-r\t" + "number of reads being generated");
			System.out.println("-i\t" + "chunk index");
			System.out.println("-k\t" + "total number of chunks");
			System.exit(0);
		}

		new RunnerPaper().run(srcDir.getAbsolutePath(), cores, numOfReads, chunkIndex, chunks);

	}

}
