/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util.graphCaner.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author bagci
 */
public class Tree {

	private ArrayList<Node> leaves;
	private Node root;
	private HashMap<Integer, Node> id2node;

	public Tree() {
		this.root = new Node(1);
		root.setRank("root");
		this.id2node = new HashMap<>();
	}

	public Node getRoot() {
		return root;
	}

	public void addNode(int id, Node n) {
		this.id2node.put(id, n);
	}

	public Node getNode(int id) {
		return this.id2node.get(id);
	}

	public boolean hasNode(int id) {
		return this.id2node.containsKey(id);
	}

	public int getNumNodes() {
		return this.id2node.size();
	}

	public List<Node> getNodes() {
		return (List<Node>) this.id2node.values();
	}

	public ArrayList<Node> getLeaves() {
		if (leaves == null) {
			leaves = new ArrayList<Node>();
			cmpLeafSetRec(root);
		}
		return leaves;
	}

	private void cmpLeafSetRec(Node v) {
		if (v.getChildren().isEmpty())
			leaves.add(v);
		else {
			for (Node c : v.getChildren())
				cmpLeafSetRec(c);
		}
	}

}
