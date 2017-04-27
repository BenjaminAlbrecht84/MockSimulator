package startUp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
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
import java.util.zip.GZIPOutputStream;

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

public class RunnerPaper {

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

	public void run(String srcDir, int cores, int numOfReads, int chunkIndex, int chunks) {

		try {

			executor = Executors.newFixedThreadPool(cores);

			File namesDMP = new File(srcDir + "/taxdump/names.dmp");
			File nodesDMP = new File(srcDir + "/taxdump/nodes.dmp");
			File csvFile = new File(srcDir + "/genomes_proks.csv");
			File genomeFolder = new File(srcDir + "/Genomes");
			File proteinFolder = new File(srcDir + "/Proteins");
			File dmndDB = new File(srcDir + "/Proteins/proteins_fullGenome.dmnd");

			int[] border = { 2, 10 };

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

			// selecting relevantTaxa
			int chunkSize = (int) Math.ceil((double) properGenusTaxa.size() / (double) chunks);
			int counter = 0;
			System.out.println("Step 6 - Processing " + chunkSize + " genomes");
			for (int i = chunkIndex * chunkSize; i < properGenusTaxa.size(); i++) {

				Node selectedNode = properGenusTaxa.get(i);
				ArrayList<Node> selectedTaxa = new ArrayList<Node>();
				selectedTaxa.add(properGenusTaxa.get(i));

				if (selectedNode.getGenomeFile() != null) {

					int complexity = selectedNode.getAncestorAtRank("genus").getNumOfFTPLeaves();

					// preparing databases
					System.out.println("Step 6." + i + ".1 - Preparing database information");
					File resFolder = new File(srcDir + "/" + selectedNode.getGenomeFile().getName().replaceAll(".fna.gz", ":" + complexity));
					if (resFolder.exists())
						deleteDir(resFolder);
					resFolder.mkdirs();
					ArrayList<Node> nonSelectedLeaves = new ArrayList<Node>();
					HashSet<SparseString> proteinIDs = new HashSet<SparseString>();
					for (Node v : ftpLeaves) {
						if (!selectedTaxa.contains(v) && v.getProteinIDs() != null && v.getGenomeFile() != null) {
							proteinIDs.addAll(v.getProteinIDs());
							nonSelectedLeaves.add(v);
						}
					}

					// creating protein database
					ArrayList<SparseString> uniqueIDs = new ArrayList<SparseString>();
					for (SparseString s : proteinIDs)
						uniqueIDs.add(s);
					File dbFile = new File(resFolder.getAbsolutePath() + "/protein_db_" + i + ".faa");
					System.out.println("Step 6." + i + ".2 - Creating protein database: " + dbFile.getAbsolutePath());
					chunk = (int) Math.ceil((double) uniqueIDs.size() / (double) cores);
					threads = new ArrayList<Runnable>();
					FileWriter writer = new FileWriter(dbFile);
					for (int j = 0; j < cores; j++)
						threads.add(new DBWriterThread(writer, dmndDB, dmndIndex, uniqueIDs, j * chunk, chunk));
					maxProgress = proteinIDs.size();
					runInParallel(threads);
					writer.close();
					reportFinish();

					// compressing protein database
					File dpFileZipped = new File(dbFile.getAbsolutePath().replaceAll(".faa", ".faa.gz"));
					System.out.println("Step 6." + i + ".3 - Creating gzipped protein database: " + dpFileZipped.getAbsolutePath());
					compressGzipFile(dbFile.getAbsolutePath(), dpFileZipped.getAbsolutePath());
					dbFile.delete();

					// creating synthetic reads
					System.out.println("Step 6." + i + ".4 - Creating synthetic reads for " + selectedNode.getGenomeFile().getName());
					simulateReads(selectedNode, resFolder, numOfReads, srcDir, genomeToFile);

				}

				counter++;
				if (counter == chunkSize)
					break;

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

	private void simulateReads(Node selectedTaxon, File resFolder, int numOfReads, String srcDir, ConcurrentHashMap<String, File> genomeToFile) {

		String ftp = (String) selectedTaxon.getInfo();
		String[] split = ftp.split("/");
		String remoteFNA_File = split[split.length - 1] + "_genomic.fna.gz";

		// getting genome file
		File source = genomeToFile.get(remoteFNA_File);
		File fnaFile = new File(resFolder.getAbsolutePath() + "/" + source.getName());
		copyFile(source, fnaFile);

		// running Nanosim on genome
		new ReadSimulator().run(fnaFile, numOfReads, srcDir);

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

	private ArrayList<Node> cmpSubset(ArrayList<Node> leaves, int[] border) {
		ArrayList<Node> relevantLeaves = new ArrayList<Node>();
		for (Node l : leaves) {
			if (hasRank(l, "genus") && hasRank(l, "family") && hasRank(l, "order") && hasRank(l, "class") && hasRank(l, "phylum")) {
				Node p = l.getAncestorAtRank("genus");
				if (p.getNumOfFTPLeaves() >= border[0] && p.getNumOfFTPLeaves() <= border[1])
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

	private static void compressGzipFile(String file, String gzipFile) {
		try {
			FileInputStream fis = new FileInputStream(file);
			FileOutputStream fos = new FileOutputStream(gzipFile);
			GZIPOutputStream gzipOS = new GZIPOutputStream(fos);
			byte[] buffer = new byte[1024];
			int len;
			while ((len = fis.read(buffer)) != -1) {
				gzipOS.write(buffer, 0, len);
			}
			// close resources
			gzipOS.close();
			fos.close();
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
