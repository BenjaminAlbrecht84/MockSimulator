/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util.graphCaner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import util.graphCaner.tree.Node;
import util.graphCaner.tree.Tree;

/**
 *
 * @author bagci
 */
public class TaxDump {

	private File taxDumpFile;
	private File namesFile;

	public TaxDump(File taxDumpFile, File namesFile) {
		this.taxDumpFile = taxDumpFile;
		this.namesFile = namesFile;
	}

	public Tree parse() {
		HashMap<Integer, String> id2Name = parseNamesFile();
		Tree tree = new Tree();
		try {
			BufferedReader br = new BufferedReader(new FileReader(taxDumpFile));
			String line = br.readLine();
			while ((line = br.readLine()) != null) {
				String[] lineSplit = line.split("\t");
				int id = Integer.parseInt(lineSplit[0]);
				int parentId = Integer.parseInt(lineSplit[2]);
				String rank = lineSplit[4];
				Node n = null;
				if (tree.hasNode(id)) {
					n = tree.getNode(id);
					n.setRank(rank);
					if (tree.hasNode(parentId)) {
						n.setParent(tree.getNode(parentId));
						tree.getNode(parentId).addChild(n);
					} else {
						Node parent = new Node(parentId);
						parent.addChild(n);
						parent.setName(id2Name.get(parentId));
						n.setParent(parent);
						tree.addNode(parentId, parent);
					}
				} else {
					n = new Node(id);
					n.setRank(rank);
					if (tree.hasNode(parentId)) {
						n.setParent(tree.getNode(parentId));
						tree.getNode(parentId).addChild(n);
					} else {
						Node parent = new Node(parentId);
						parent.addChild(n);
						parent.setName(id2Name.get(parentId));
						tree.addNode(parentId, parent);
						n.setParent(parent);
					}
					n.setName(id2Name.get(id));
					tree.addNode(id, n);
				}
				if (rank.equals("superkingdom")) {
					tree.getRoot().addChild(n);
					n.setParent(tree.getRoot());
				}
			}
			br.close();
		} catch (FileNotFoundException ex) {
			Logger.getLogger(TaxDump.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(TaxDump.class.getName()).log(Level.SEVERE, null, ex);
		}
		Node unknown = new Node(-1);
		unknown.setParent(tree.getNode(1));
		tree.getNode(1).addChild(unknown);
		tree.addNode(-1, unknown);
		return tree;
	}

	private HashMap<Integer, String> parseNamesFile() {
		HashMap<Integer, String> id2Name = new HashMap<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(namesFile));
			String line = "";

			while ((line = br.readLine()) != null) {
				String[] lineSplit = line.split("\t");
				// System.out.println(Arrays.toString(lineSplit));
				if (lineSplit[6].equals("scientific name")) {
					id2Name.put(Integer.parseInt(lineSplit[0]), lineSplit[2]);
				}
			}
			br.close();
			return id2Name;

		} catch (FileNotFoundException ex) {
			Logger.getLogger(TaxDump.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(TaxDump.class.getName()).log(Level.SEVERE, null, ex);
		}
		return null;
	}

}
