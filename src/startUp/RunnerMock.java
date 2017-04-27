package startUp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import util.AA_Alphabet;
import util.CSV_Parser;
import util.Dmnd_IndexReader;
import util.FAA_Reader;
import util.ReadSimulator;
import util.SparseString;
import util.graphCaner.TaxDump;
import util.graphCaner.tree.Node;
import util.graphCaner.tree.Tree;

public class RunnerMock {

	private static HashMap<Integer, Character> indexToAA;
	static {
		indexToAA = new HashMap<Integer, Character>();
		for (int i = 0; i < AA_Alphabet.getAaString().length(); i++)
			indexToAA.put(i, AA_Alphabet.getAaString().charAt(i));
	}

	private int maxProgress, lastProgress = 0;
	private AtomicInteger progress = new AtomicInteger();

	private CountDownLatch latch;
	private ExecutorService executor;

	public void run(String srcDir, int cores) {

		try {

			executor = Executors.newFixedThreadPool(cores);

			File namesDMP = new File(srcDir + "taxdump/names.dmp");
			File nodesDMP = new File(srcDir + "taxdump/nodes.dmp");
			File csvFile = new File(srcDir + "genomes_proks.csv");
			File genomeFolder = new File(srcDir + "Genomes");
			File proteinFolder = new File(srcDir + "Proteins");
			File dmndDB = new File(srcDir + "Proteins/proteins_fullGenome.dmnd");

			int numOfMocks = 10;
			int[] border = { 2, 20 };
			int highAbundance = 18000 / 8;
			int lowAbundance = 2000 / 8;

			// creating taxonomic tree
			System.out.println("Step 1 - Parsing taxonomic tree from " + namesDMP.getAbsolutePath() + " and " + nodesDMP.getAbsolutePath());
			Tree t = new TaxDump(nodesDMP, namesDMP).parse();

			// adding FTP address to leaves
			System.out.println("Step 2 - Parsing CSV file " + csvFile.getAbsolutePath());
			HashMap<String, String> taxonToFTP = CSV_Parser.run(csvFile);
			ArrayList<Node> ftpLeaves = new ArrayList<Node>();
			for (Node l : t.getLeaves()) {
				if (taxonToFTP.containsKey(l.getName())) {
					l.setInfo(taxonToFTP.get(l.getName()));
					ftpLeaves.add(l);
					l.reportFTPLeaf();
				}
			}

			// filtering relevant taxa
			System.out.println("Step 3 - Filtering taxa based on interval: [" + border[0] + "," + border[1] + "]");
			ArrayList<Node> properGenusTaxa = cmpSubset(ftpLeaves, border);
			System.out.println("Filtered: " + properGenusTaxa.size());

			int counter = 0;
			ArrayList<String> names = new ArrayList<String>();
			for (Node v : properGenusTaxa) {
				if (!names.contains(v.getAncestorAtRank("order").getName())) {
					counter++;
					names.add(v.getAncestorAtRank("order").getName());
					if (v.getAncestorAtRank("order").getChildren().size() < 3)
						System.out.println(v.getAncestorAtRank("order").getChildren().size());
				}
			}
			System.out.println(counter);

			// retrieving protein and genome information
			System.out.println("Step 4 - Getting sequence information for " + ftpLeaves.size() + " genome(s)");
			proteinFolder.mkdir();
			ConcurrentHashMap<String, File> proteinToFile = new ConcurrentHashMap<String, File>();
			for (File f : proteinFolder.listFiles())
				proteinToFile.put(f.getName(), f);
			ConcurrentHashMap<String, File> genomeToFile = new ConcurrentHashMap<String, File>();
			genomeFolder.mkdir();
			for (File f : genomeFolder.listFiles())
				genomeToFile.put(f.getName(), f);
			int chunk = (int) Math.ceil((double) ftpLeaves.size() / (double) cores);
			ArrayList<Runnable> threads = new ArrayList<Runnable>();
			for (int i = 0; i < cores; i++)
				threads.add(new SequenceThread(ftpLeaves, i * chunk, chunk, proteinFolder, genomeFolder, proteinToFile, genomeToFile));
			maxProgress = ftpLeaves.size();
			runInParallel(threads);
			reportFinish();

			// loading dmnd file
			System.out.println("Step 5 - Loading index for protein sequences: " + dmndDB.getAbsolutePath());
			Dmnd_IndexReader dmndIndex = new Dmnd_IndexReader(dmndDB);
			dmndIndex.createIndex();
			reportFinish();

			System.out.println("Step 6 - Creating " + numOfMocks + " mock datasets");
			for (int mockNumber = 0; mockNumber < numOfMocks; mockNumber++) {

				// selecting relevantTaxa
				System.out.println("Step 6." + mockNumber + ".1 - Selecting " + 30 + " taxa");
				ArrayList<Node> selectedTaxa = randSelect(properGenusTaxa, border);

				// // preparing databases
				// System.out.println("Step 6." + mockNumber + ".2 - Creating database");
				// File resFolder = new File(srcDir + "/syn_mockdataset_" + mockNumber);
				// if (resFolder.exists())
				// deleteDir(resFolder);
				// resFolder.mkdirs();
				// ArrayList<Node> nonSelectedLeaves = new ArrayList<Node>();
				// HashSet<SparseString> proteinIDs = new HashSet<SparseString>();
				// for (Node v : ftpLeaves) {
				// if (!selectedTaxa.contains(v) && v.getProteinIDs() != null && v.getGenomeFile() != null) {
				// proteinIDs.addAll(v.getProteinIDs());
				// nonSelectedLeaves.add(v);
				// }
				// }
				//
				// // creating genome database
				// File selGenomeFolder = new File(resFolder.getAbsolutePath() + "/selected_genomes" + mockNumber);
				// File genomeDBFolder = new File(resFolder.getAbsolutePath() + "/genome_db_" + mockNumber);
				// genomeDBFolder.mkdir();
				// selGenomeFolder.mkdir();
				// System.out.println("Creating genome database: " + genomeDBFolder.getAbsolutePath());
				// chunk = (int) Math.ceil((double) nonSelectedLeaves.size() / (double) cores);
				// threads = new ArrayList<Runnable>();
				// for (int i = 0; i < cores; i++)
				// threads.add(new CopyGenomeThread(nonSelectedLeaves, genomeDBFolder, i * chunk, chunk, false));
				// threads.add(new CopyGenomeThread(selectedTaxa, selGenomeFolder, 0, selectedTaxa.size(), true));
				// maxProgress = nonSelectedLeaves.size();
				// runInParallel(threads);
				// reportFinish();
				//
				// // creating protein database
				// ArrayList<SparseString> uniqueIDs = new ArrayList<SparseString>();
				// for (SparseString s : proteinIDs)
				// uniqueIDs.add(s);
				// File dbFile = new File(resFolder.getAbsolutePath() + "/mock_db_" + mockNumber + ".faa");
				// System.out.println("Creating protein database: " + dbFile.getAbsolutePath());
				// chunk = (int) Math.ceil((double) uniqueIDs.size() / (double) cores);
				// threads = new ArrayList<Runnable>();
				// FileWriter writer = new FileWriter(dbFile);
				// for (int i = 0; i < cores; i++)
				// threads.add(new DBWriterThread(writer, dmndDB, dmndIndex, uniqueIDs, i * chunk, chunk));
				// maxProgress = proteinIDs.size();
				// runInParallel(threads);
				// writer.close();
				// reportFinish();
				//
				// // creating synthetic reads
				// System.out.println("Step 6." + mockNumber + ".3 - Creating synthetic reads for " + selectedTaxa.size() + " taxa");
				// chunk = (int) Math.ceil((double) selectedTaxa.size() / (double) cores);
				// threads = new ArrayList<Runnable>();
				// for (int i = 0; i < cores; i++)
				// threads.add(
				// new ReadSimulatorThread(selectedTaxa, resFolder, highAbundance, lowAbundance, i * chunk, chunk, srcDir, genomeToFile));
				// maxProgress = selectedTaxa.size();
				// runInParallel(threads);
				// reportFinish();

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		executor.shutdown();

	}

	public class CopyGenomeThread implements Runnable {

		private ArrayList<Node> taxa;
		private File genomeDBFolder;
		private int start, chunk;
		private boolean addComplexity;

		public CopyGenomeThread(ArrayList<Node> taxa, File genomeDBFolder, int start, int chunk, boolean addComplexity) {
			this.taxa = taxa;
			this.genomeDBFolder = genomeDBFolder;
			this.start = start;
			this.chunk = chunk;
			this.addComplexity = addComplexity;
		}

		public void run() {
			try {
				int counter = 0;
				for (int i = start; i < taxa.size(); i++) {

					File source = taxa.get(i).getGenomeFile();
					File target = new File(genomeDBFolder.getAbsolutePath() + "/" + source.getName());
					if (addComplexity) {
						Node p = taxa.get(i).getAncestorAtRank("genus");
						target = new File(genomeDBFolder.getAbsolutePath() + "/"
								+ source.getName().replace(".fna.gz", ":" + p.getNumOfFTPLeaves() + ".fna.gz"));
					}
					copyFile(source, target);

					counter++;
					if (counter % 100 == 0)
						reportProgress(100);
					if (counter == chunk)
						break;

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			latch.countDown();
		}

	}

	private void copyFile(File source, File target) {
		FileChannel sourceChannel = null;
		FileChannel destChannel = null;
		try {
			try {
				sourceChannel = new FileInputStream(source).getChannel();
				destChannel = new FileOutputStream(target).getChannel();
				destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
			} finally {
				sourceChannel.close();
				destChannel.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public class ReadSimulatorThread implements Runnable {

		private ArrayList<Node> selectedTaxa;
		private File resFolder;
		private int highAbundance, lowAbundance;
		private int start, chunk;
		private String srcDir;
		private ConcurrentHashMap<String, File> genomeToFile;

		public ReadSimulatorThread(ArrayList<Node> selectedTaxa, File resFolder, int highAbundance, int lowAbundance, int start, int chunk,
				String srcDir, ConcurrentHashMap<String, File> genomeToFile) {
			this.selectedTaxa = selectedTaxa;
			this.resFolder = resFolder;
			this.highAbundance = highAbundance;
			this.lowAbundance = lowAbundance;
			this.start = start;
			this.chunk = chunk;
			this.srcDir = srcDir;
			this.genomeToFile = genomeToFile;
		}

		public void run() {
			int counter = 0;
			Random rand = new Random();
			for (int i = start; i < selectedTaxa.size(); i++) {

				String ftp = (String) selectedTaxa.get(i).getInfo();
				String remoteFolder = ftp.replaceAll("ftp://ftp.ncbi.nlm.nih.gov", "");
				String[] split = ftp.split("/");
				String remoteFNA_File = split[split.length - 1] + "_genomic.fna.gz";

				// getting genome file
				File source = genomeToFile.get(remoteFNA_File);
				File fnaFile = new File(resFolder.getAbsolutePath() + "/" + source.getName());
				copyFile(source, fnaFile);

				// running Nanosim on genome
				int numOfReads = i < selectedTaxa.size() / 2 ? highAbundance : lowAbundance;
				int var = (int) Math.round(0.5 * (double) numOfReads);
				int offset = rand.nextInt(2) == 0 ? -rand.nextInt(var) : rand.nextInt(var);
				numOfReads += offset;

				new ReadSimulator().run(fnaFile, numOfReads, srcDir);
				counter++;
				if (counter == chunk)
					break;

				reportProgress(1);

			}
			latch.countDown();
		}

	}

	private FTPClient setUpFTPClient() {

		String userId = "anonymous";
		String password = "";

		try {

			// new ftp client
			FTPClient ftpClient = new FTPClient();

			// try to connect
			ftpClient.connect("ftp.ncbi.nlm.nih.gov");

			// login to server
			if (!ftpClient.login(userId, password)) {
				ftpClient.logout();
				return null;
			}
			int reply = ftpClient.getReplyCode();

			// FTPReply stores a set of constants for FTP reply codes.
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				return null;
			}

			// enter passive mode
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);

			return ftpClient;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public class DBWriterThread implements Runnable {

		private FileWriter writer;
		private File refFile;
		private Dmnd_IndexReader dmndReader;
		private ArrayList<SparseString> gIs;
		private int start, chunk;

		public DBWriterThread(FileWriter writer, File refFile, Dmnd_IndexReader dmndReader, ArrayList<SparseString> gIs, int start, int chunk) {
			this.writer = writer;
			this.refFile = refFile;
			this.dmndReader = dmndReader;
			this.gIs = gIs;
			this.start = start;
			this.chunk = chunk;
		}

		public void run() {
			try {
				RandomAccessFile raf = new RandomAccessFile(refFile, "r");
				try {
					int counter = 0;
					StringBuilder out = new StringBuilder();
					for (int i = start; i < gIs.size(); i++) {
						SparseString gi = gIs.get(i);
						SparseString accession = new SparseString(gi.toString().split(" ")[0]);
						Long loc = dmndReader.getGILocation(accession);
						if (loc != null) {
							loc += 1;
							raf.seek(loc);
							ByteBuffer buffer = ByteBuffer.allocate(1024);
							buffer.order(ByteOrder.LITTLE_ENDIAN);
							int readChars = 0;
							boolean doBreak = false;
							StringBuffer aaSeq = new StringBuffer();
							while ((readChars = raf.read(buffer.array())) != -1 && !doBreak) {
								for (int r = 0; r < readChars; r++) {
									int aaIndex = (int) buffer.get(r);
									if (doBreak = (aaIndex == -1))
										break;
									if (indexToAA.containsKey(aaIndex))
										aaSeq = aaSeq.append(indexToAA.get(aaIndex));
									else
										aaSeq = aaSeq.append("X");
								}
							}

							out.append(">" + gi.toString() + "\n" + aaSeq + "\n");

						} else
							System.err.println("WARNING: accession not in dmnd File: " + gi.toString() + " " + accession);

						counter++;
						if (counter % 1000 == 0 || counter == gIs.size() || counter == chunk) {
							writer.write(out.toString());
							out = new StringBuilder();
							reportProgress(1000);
						}

						if (counter == chunk)
							break;

					}
				} finally {
					raf.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			latch.countDown();
		}

	}

	public class SequenceThread implements Runnable {

		private ArrayList<Node> leaves;
		private File proteinFolder, genomeFolder;
		private ConcurrentHashMap<String, File> proteinToFile, genomeToFile;
		private int start, chunk;

		public SequenceThread(ArrayList<Node> leaves, int start, int chunk, File proteinFolder, File genomeFolder,
				ConcurrentHashMap<String, File> proteinToFile, ConcurrentHashMap<String, File> genomeToFile) {
			this.leaves = leaves;
			this.proteinFolder = proteinFolder;
			this.genomeFolder = genomeFolder;
			this.proteinToFile = proteinToFile;
			this.genomeToFile = genomeToFile;
			this.start = start;
			this.chunk = chunk;
		}

		@Override
		public void run() {

			try {

				// new ftp client
				FTPClient ftpClient = setUpFTPClient();

				int counter = 0, delta = 50;
				for (int i = start; i < leaves.size(); i++) {

					Node l = leaves.get(i);
					String ftp = (String) l.getInfo();
					if (ftp != null) {

						String remoteFolder = ftp.replaceAll("ftp://ftp.ncbi.nlm.nih.gov", "");
						String[] split = ftp.split("/");
						String remoteFAA_File = split[split.length - 1] + "_protein.faa.gz";
						String remoteFNA_File = split[split.length - 1] + "_genomic.fna.gz";

						File faa_file = proteinToFile.get(remoteFAA_File);
						// if (faa_file == null)
						// faa_file = Downloader.startFTP(ftpClient, remoteFolder, remoteFAA_File, proteinFolder);
						if (faa_file != null)
							l.setProteinIDs(FAA_Reader.read(faa_file));

						File fna_file = genomeToFile.get(remoteFNA_File);
						// if (fna_file == null)
						// fna_file = Downloader.startFTP(ftpClient, remoteFolder, remoteFNA_File, genomeFolder);
						if (fna_file != null)
							l.setGenomeFile(fna_file);

					}

					if (counter % delta == 0)
						reportProgress(delta);

					counter++;
					if (counter == chunk)
						break;

				}

			} catch (Exception e) {
				latch.countDown();
				e.printStackTrace();
			}

			latch.countDown();

		}

	}

	private void runInParallel(ArrayList<Runnable> threads) {
		latch = new CountDownLatch(threads.size());
		for (Runnable t : threads)
			executor.submit(t);
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private ArrayList<Node> randSelect(ArrayList<Node> filteredTaxa, int[] border) {
		ArrayList<Node> selectedTaxa = new ArrayList<Node>();
		Random rand = new Random();

		while (selectedTaxa.size() != 30) {

			selectedTaxa = new ArrayList<Node>();

			// selected guiding leaves with different phylum nodes
			Node l1 = filteredTaxa.get(rand.nextInt(filteredTaxa.size() - 1));
			Node l2 = filteredTaxa.get(rand.nextInt(filteredTaxa.size() - 1));
			while (l1.getAncestorAtRank("class").getName().equals(l2.getAncestorAtRank("class").getName())) {
				l1 = filteredTaxa.get(rand.nextInt(filteredTaxa.size() - 1));
				l2 = filteredTaxa.get(rand.nextInt(filteredTaxa.size() - 1));
			}

			// select ftp leaves for each l1 and l2
			selectRankNodes(l1, selectedTaxa, rand, border);
			selectRankNodes(l2, selectedTaxa, rand, border);

		}

		return selectedTaxa;
	}

	private void selectRankNodes(Node v, ArrayList<Node> selectedTaxa, Random rand, int[] border) {
		String[] ranks = { "genus", "family", "order", "class" };
		Node lastNode = v;
		for (String rank : ranks) {
			v = v.getAncestorAtRank(rank);
			for (Node c : v.getChildren()) {
				ArrayList<Node> ftpLeaves = new ArrayList<Node>();
				getProperFTPLeavesRec(c, lastNode, ftpLeaves, border);
				if (!ftpLeaves.isEmpty()) {
					Node l = ftpLeaves.get(rand.nextInt(ftpLeaves.size()));
					selectedTaxa.add(l);
				}
				if (selectedTaxa.size() == 2)
					break;
			}
			lastNode = v;
		}
	}

	private int getNumOfProperFTPLeaves(Node v, Node stopNode, int[] border) {
		ArrayList<Node> properFTPLeaves = new ArrayList<Node>();
		getProperFTPLeavesRec(v, stopNode, properFTPLeaves, border);
		return properFTPLeaves.size();
	}

	private void getProperFTPLeavesRec(Node v, Node stopNode, ArrayList<Node> properFTPLeaves, int[] border) {
		if (v != stopNode) {
			if (v.getChildren().isEmpty() && v.getInfo() != null) {
				boolean rankCheck = hasRank(v, "genus") && hasRank(v, "family") && hasRank(v, "order") && hasRank(v, "class");
				if (rankCheck)
					properFTPLeaves.add(v);
			} else if (v.getNumOfFTPLeaves() > 1) {
				if (!v.getRank().equals("genus") || (v.getNumOfFTPLeaves() >= border[0] && v.getNumOfFTPLeaves() <= border[1])) {
					for (Node c : v.getChildren())
						getProperFTPLeavesRec(c, stopNode, properFTPLeaves, border);
				}
			}
		}
	}

	private boolean checkRank(Node v, Node w, int[] border) {
		if (v.getChildren().size() >= 3) {
			int counter = 0;
			for (Node c : v.getChildren()) {
				if (getNumOfProperFTPLeaves(c, w, border) > 0)
					counter++;
			}
			if (counter >= 2)
				return true;
		}
		return false;
	}

	private ArrayList<Node> cmpSubset(ArrayList<Node> leaves, int[] border) {
		ArrayList<Node> relevantLeaves = new ArrayList<Node>();

		for (Node l : leaves) {
			if (hasRank(l, "genus") && hasRank(l, "family") && hasRank(l, "order") && hasRank(l, "class")) {
				boolean genusCheck = checkRank(l.getAncestorAtRank("genus"), null, border);
				boolean familyCheck = checkRank(l.getAncestorAtRank("family"), l.getAncestorAtRank("genus"), border);
				boolean orderCheck = checkRank(l.getAncestorAtRank("order"), l.getAncestorAtRank("family"), border);
				boolean classCheck = checkRank(l.getAncestorAtRank("class"), l.getAncestorAtRank("order"), border);
				Node p = l.getAncestorAtRank("genus");
				if (genusCheck && familyCheck && orderCheck && classCheck
						&& (p.getNumOfFTPLeaves() >= border[0] && p.getNumOfFTPLeaves() <= border[1]))
					relevantLeaves.add(l);
			}
		}
		return relevantLeaves;
	}

	private boolean hasRank(Node l, String rank) {
		return l.getAncestorAtRank(rank) != null;
	}

	private void reportProgress(int delta) {
		progress.getAndAdd(delta);
		int p = ((int) ((((double) progress.get() / (double) maxProgress)) * 100) / 10) * 10;
		if (p > lastProgress && p < 100) {
			lastProgress = p;
			System.out.print(p + "% ");
		}
	}

	private void reportFinish() {
		progress.set(0);
		lastProgress = 0;
		System.out.print(100 + "%\n");
	}

	public boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}

		return dir.delete(); // The directory is empty now and can be deleted.
	}

}
