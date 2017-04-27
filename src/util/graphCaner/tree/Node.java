/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util.graphCaner.tree;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;

import util.SparseString;

/**
 *
 * @author bagci
 */
public class Node {

	private int taxid;
	private ArrayList<Edge> outgoingEdges;
	private Node parent;
	private ArrayList<Node> children;
	private String rank;
	private ArrayList<String> accessions;
	private int count;
	private String name;

	private Object info;
	private int numOfFTPLeaves = 0;
	private ArrayList<SparseString> proteinIDs;
	private File genomeFile;

	public Node(int taxid) {
		this.taxid = taxid;
		outgoingEdges = new ArrayList<>();
		this.children = new ArrayList<>();
		this.accessions = new ArrayList<>();
		this.count = 0;
	}

	public void addEdge(Edge e) {
		this.outgoingEdges.add(e);
	}

	public ArrayList<Edge> getOutgoingEdges() {
		return outgoingEdges;
	}

	public int getTaxid() {
		return taxid;
	}

	public void addChild(Node n) {
		this.children.add(n);
	}

	public void setParent(Node parent) {
		this.parent = parent;
	}

	public void setTaxid(int taxid) {
		this.taxid = taxid;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}

	public String getRank() {
		return rank;
	}

	public ArrayList<Node> getChildren() {
		return children;
	}

	public Node getParent() {
		return parent;
	}

	public boolean isLeaf() {
		return this.children.isEmpty();
	}

	public void addAccession(String acc) {
		this.accessions.add(acc);
	}

	public ArrayList<String> getAccessions() {
		return accessions;
	}

	public void incrementCount() {
		this.count++;
	}

	public int getCount() {
		return this.count;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Object getInfo() {
		return info;
	}

	public void setInfo(Object info) {
		this.info = info;
	}

	public void reportFTPLeaf() {
		if (!children.isEmpty())
			numOfFTPLeaves++;
		if (parent != null)
			parent.reportFTPLeaf();
	}

	public int getNumOfFTPLeaves() {
		return numOfFTPLeaves;
	}

	public String getAncestors() {
		boolean traverse = true;
		Node currentNode = this;
		String ret = currentNode.getName() + ";";
		while (traverse) {
			if (currentNode.getParent() == null) {
				traverse = false;
				return ret;
			} else {
				ret = currentNode.getParent().getName() + ";" + ret;
				currentNode = currentNode.getParent();
			}
		}
		return ret;

	}

	public HashSet<Integer> getAncestorIds() {
		boolean traverse = true;
		Node currentNode = this;
		HashSet<Integer> ret = new HashSet<Integer>();
		while (traverse) {
			if (currentNode.getParent() == null) {
				traverse = false;
				return ret;
			} else {
				ret.add(currentNode.getParent().taxid);
				currentNode = currentNode.getParent();
			}
		}
		return ret;

	}

	public Node getAncestorAtRank(String rank) {
		boolean traverse = true;
		Node currentNode = this;
		while (traverse) {
			if (currentNode.getRank().equals(rank)) {
				return currentNode;
			}
			if (currentNode.getParent() == null) {
				traverse = false;
				return null;
			} else {
				currentNode = currentNode.getParent();
			}
		}
		return null;
	}

	public void setProteinIDs(ArrayList<SparseString> proteinIDs) {
		this.proteinIDs = proteinIDs;
	}

	public ArrayList<SparseString> getProteinIDs() {
		return proteinIDs;
	}

	public void setGenomeFile(File fna_file) {
		this.genomeFile = fna_file;
	}

	public File getGenomeFile() {
		return genomeFile;
	}

}
