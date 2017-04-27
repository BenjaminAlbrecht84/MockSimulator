package startUp;

import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import io.FileReader_Simple;
import util.AA_Alphabet;
import util.CSV_Parser;
import util.Dmnd_IndexReader;
import util.FAA_Reader;
import util.ReadSimulator;
import util.SparseString;
import util.Downloader;
import util.graph.MyNode;
import util.graph.MyPhyloTree;

public class Runner {

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

	public void run() {

		int cores = 8;
		executor = Executors.newFixedThreadPool(cores);

		String srcDir = "/Users/Benjamin/Documents/Uni_Tuebingen/LongReads/syn_evaluation/NCBI_taxonomy/";
		File phylipFile = new File(srcDir + "/phyliptree.phy");
		File csvFile = new File(srcDir + "/genomes_proks.csv");
		File proteinFolder = new File(srcDir + "/Proteins");
		File dmndDB = new File(srcDir + "/Proteins/prokaryotes_fullGenome.dmnd");

		int[] border = { 2, 10 };
		int numOfGenomes = 10;
		int numOfReadsPerGenome = 1000;

		// creating taxonomic tree
		System.out.println("Step 1 - Parsing taxonomic tree from " + phylipFile.getAbsolutePath());
		MyPhyloTree t = new MyPhyloTree();
		t.parseBracketNotation(FileReader_Simple.run(phylipFile));

		// adding FTP address to leaves
		System.out.println("Step 2 - Parsing CSV file " + csvFile.getAbsolutePath());
		HashMap<String, String> taxonToFTP = CSV_Parser.run(csvFile);
		for (MyNode l : t.getLeaves())
			l.setInfo(taxonToFTP.get(l.getLabel()));

		// filtering relevant taxa
		System.out.println("Step 3 - Filtering taxa based on interval: [" + border[0] + "," + border[1] + "]");
		ArrayList<MyNode> filteredTaxa = cmpSubset(t.getLeaves(), border);

		// selecting relevantTaxa
		System.out.println("Step 4 - Selecting " + numOfGenomes + " taxa");
		ArrayList<MyNode> selectedTaxa = randSelect(filteredTaxa, numOfGenomes);

		// retrieving protein information
		System.out.println("Step 5 - Getting protein information for " + t.getLeaves().size() + " genome(s)");
		proteinFolder.mkdir();
		HashMap<String, File> nameToFile = new HashMap<String, File>();
		for (File f : proteinFolder.listFiles())
			nameToFile.put(f.getName(), f);
		int chunk = (int) Math.ceil((double) t.getLeaves().size() / (double) cores);
		ArrayList<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < cores; i++)
			threads.add(new ProteinThread(t.getLeaves(), i * chunk, chunk, proteinFolder, nameToFile));
		maxProgress = t.getLeaves().size();
		runInParallel(threads);
		reportFinish();

		// creating index
		System.out.println("Step 6 - Loading index for protein sequences");
		Dmnd_IndexReader dmndIndex = new Dmnd_IndexReader(dmndDB);
		dmndIndex.createIndex();
		reportFinish();

		// creating synthetic reads and databases
		System.out.println("Step 7 - Creating synthetic reads and databases");
		for (MyNode l : selectedTaxa) {

			String ftp = (String) l.getInfo();
			String remoteFolder = ftp.replaceAll("ftp://ftp.ncbi.nlm.nih.gov", "");
			String[] split = ftp.split("/");
			String remoteFNA_File = split[split.length - 1] + "_genomic.fna.gz";
			File resFolder = new File(srcDir + "/" + split[split.length - 1]);
			if (resFolder.exists())
				deleteDir(resFolder);
			resFolder.mkdirs();

			// creating reference database
			HashSet<SparseString> proteinIDs = new HashSet<SparseString>();
			for (MyNode v : t.getLeaves()) {
				if (v != l && v.getProteinIDs() != null)
					proteinIDs.addAll(v.getProteinIDs());
			}
			File dbFile = new File(resFolder.getAbsolutePath() + "/" + split[split.length - 1] + "_db.faa");
			System.out.println("Creating database: " + dbFile.getAbsolutePath());
			maxProgress = proteinIDs.size();
			writeOutReferences(proteinIDs, dmndDB, dmndIndex, dbFile);
			reportFinish();

			// getting genome file
			File fnaFile = Downloader.startFTP(setUpFTPClient(), remoteFolder, remoteFNA_File, resFolder);

			// running Nanosim on genomes
			System.out.println("Simulating reads: " + fnaFile.getAbsolutePath());
			new ReadSimulator().run(fnaFile, numOfReadsPerGenome, srcDir);

		}

		executor.shutdown();

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

	private void writeOutReferences(HashSet<SparseString> gIs, File refFile, Dmnd_IndexReader dmndReader, File dbFile) {

		try {
			FileWriter writer = new FileWriter(dbFile, true);
			RandomAccessFile raf = new RandomAccessFile(refFile, "r");
			try {
				int counter = 0;
				StringBuilder out = new StringBuilder();
				for (SparseString gi : gIs) {
					Long loc = dmndReader.getGILocation(gi);
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

						counter++;
						if (counter % 1000 == 0 || counter == gIs.size()) {
							writer.write(out.toString());
							out = new StringBuilder();
							reportProgress(1000);
						}

					}
				}
			} finally {
				writer.close();
				raf.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public class ProteinThread extends Thread {

		private ArrayList<MyNode> leaves;
		private File proteinFolder;
		private HashMap<String, File> nameToFile;
		private int start, chunk;

		public ProteinThread(ArrayList<MyNode> leaves, int start, int chunk, File proteinFolder, HashMap<String, File> nameToFile) {
			this.leaves = leaves;
			this.proteinFolder = proteinFolder;
			this.nameToFile = nameToFile;
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

					MyNode l = leaves.get(i);
					String ftp = (String) l.getInfo();
					if (ftp != null) {

						String remoteFolder = ftp.replaceAll("ftp://ftp.ncbi.nlm.nih.gov", "");
						String[] split = ftp.split("/");
						String remoteFAA_File = split[split.length - 1] + "_protein.faa.gz";

						File faa_file = nameToFile.containsKey(remoteFAA_File) ? nameToFile.get(remoteFAA_File)
								: Downloader.startFTP(ftpClient, remoteFolder, remoteFAA_File, proteinFolder);
						if (faa_file != null)
							l.setProteinIDs(FAA_Reader.read(faa_file));
					}

					counter++;
					if (counter % delta == 0)
						reportProgress(delta);

					if (counter == chunk)
						break;

				}

				ftpClient.logout();
				ftpClient.disconnect();

			} catch (Exception e) {
				e.printStackTrace();
			}

			latch.countDown();

		}

	}

	private void runInParallel(ArrayList<Thread> threads) {
		latch = new CountDownLatch(threads.size());
		for (Thread t : threads)
			executor.execute(t);
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private ArrayList<MyNode> randSelect(ArrayList<MyNode> filteredTaxa, int number) {
		ArrayList<MyNode> selectedTaxa = new ArrayList<MyNode>();
		Random rand = new Random();
		while (selectedTaxa.size() < number && !filteredTaxa.isEmpty()) {
			MyNode taxon = filteredTaxa.get(rand.nextInt(filteredTaxa.size()));
			selectedTaxa.add(taxon);
			filteredTaxa.remove(taxon);
		}
		return selectedTaxa;
	}

	private ArrayList<MyNode> cmpSubset(ArrayList<MyNode> leaves, int[] b) {
		ArrayList<MyNode> relevantLeaves = new ArrayList<MyNode>();
		for (MyNode l : leaves) {

			 int complexity = getGenusComplexitiy(l);
			 if (complexity >= b[0] && complexity <= b[1])
			 relevantLeaves.add(l);

		}
		return relevantLeaves;
	}

	private int getGenusComplexitiy(MyNode l) {
		MyNode p = l.getInEdges().next().getSource();
		return p.getOutDegree();
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
