package util.graph;

import java.util.Hashtable;
import java.util.Iterator;

import util.SparseString;

import java.io.File;
import java.util.ArrayList;

public class MyNode {

	private ArrayList<MyEdge> inEdges = new ArrayList<MyEdge>();
	private ArrayList<MyEdge> outEdges = new ArrayList<MyEdge>();

	private ArrayList<SparseString> proteinIDs;

	private MyGraph owner;
	private Object info;
	private String label;

	public MyNode(MyGraph owner, String label) {
		this.owner = owner;
		this.label = label;
	}

	public MyGraph getOwner() {
		return owner;
	}

	public int getOutDegree() {
		return outEdges.size();
	}

	public int getInDegree() {
		return inEdges.size();
	}

	public int getDegree() {
		return inEdges.size() + outEdges.size();
	}

	public void addInEdge(MyEdge e) {
		if (e.getTarget().equals(this))
			inEdges.add(e);
	}

	public void removeOutEdge(MyEdge e) {
		if (outEdges.contains(e))
			outEdges.remove(e);
	}

	public Iterator<MyEdge> getOutEdges() {
		return outEdges.iterator();
	}

	public void addOutEdge(MyEdge e) {
		if (e.getSource().equals(this))
			outEdges.add(e);
	}

	public void removeInEdge(MyEdge e) {
		if (inEdges.contains(e))
			inEdges.remove(e);
	}

	public Iterator<MyEdge> getInEdges() {
		return inEdges.iterator();
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String s) {
		label = s;
	}

	public void setOwner(MyPhyloTree t) {
		owner = t;
	}

	public Object getInfo() {
		return info;
	}

	public void setInfo(Object info) {
		this.info = info;
	}

	public String toNewick(String newickString, ArrayList<MyNode> visitedNodes, Hashtable<MyNode, String> nodeToHTag) {
		if (!visitedNodes.contains(this)) {
			visitedNodes.add(this);
			if (outEdges.isEmpty() && inEdges.isEmpty())
				return "(" + getLabelString() + ")";
			else if (outEdges.isEmpty()) {
				if (inEdges.size() < 2)
					return getLabelString();
				else {
					String hTag = "#H" + nodeToHTag.keySet().size();
					nodeToHTag.put(this, hTag);
					return getLabelString() + hTag + ":0.0";
				}
			} else {
				String subString = "(";
				for (MyEdge e : outEdges) {
					MyNode c = e.getTarget();
					String hTag = "";
					if (nodeToHTag.containsKey(c))
						hTag = nodeToHTag.get(c) + ":0.0";
					if (outEdges.get(outEdges.size() - 1).equals(e))
						subString = subString.concat(c.toNewick(newickString, visitedNodes, nodeToHTag) + hTag + ")");
					else
						subString = subString.concat(c.toNewick(newickString, visitedNodes, nodeToHTag) + hTag + ",");
				}
				if (inEdges.size() < 2)
					return subString + getLabelString();
				else {
					String hTag = "#H" + nodeToHTag.keySet().size();
					nodeToHTag.put(this, hTag);
					return subString + getLabelString() + hTag + ":0.0";
				}
			}
		} else
			return getLabelString();
	}

	public String toMyNewick(String newickString, ArrayList<MyNode> visitedNodes, Hashtable<MyNode, String> nodeToHTag, MyEdge inEdge) {
		if (!visitedNodes.contains(this)) {
			visitedNodes.add(this);
			if (outEdges.isEmpty() && inEdges.isEmpty())
				return "(" + getLabelString() + ")";
			else if (outEdges.isEmpty()) {
				if (inEdges.size() < 2) {
					String l = getLabelString() + getEdgeString(inEdge);
					return l;
				} else {
					String hTag = "#H" + nodeToHTag.keySet().size();
					nodeToHTag.put(this, hTag);
					String l = getLabelString() + hTag + getEdgeString(inEdge);
					return l;
				}
			} else {
				String subString = "(";
				for (MyEdge e : outEdges) {
					MyNode c = e.getTarget();
					String hTag = "";
					if (nodeToHTag.containsKey(c))
						hTag = nodeToHTag.get(c) + getEdgeString(e);
					if (outEdges.get(outEdges.size() - 1).equals(e))
						subString = subString.concat(c.toMyNewick(newickString, visitedNodes, nodeToHTag, e) + hTag + ")");
					else
						subString = subString.concat(c.toMyNewick(newickString, visitedNodes, nodeToHTag, e) + hTag + ",");
				}
				if (inEdges.size() < 2) {
					String l = subString + getLabelString() + getEdgeString(inEdge);
					return l;
				} else {
					String hTag = "#H" + nodeToHTag.keySet().size();
					nodeToHTag.put(this, hTag);
					String l = subString + getLabelString() + hTag + getEdgeString(inEdge);
					return l;
				}
			}
		} else {
			String l = getLabelString();
			return l;
		}
	}

	private String getLabelString() {
		if (label == null)
			return "";
		return label;
	}

	private String getEdgeString(MyEdge e) {
		if (e == null)
			return "";
		return e.getMyNewickString();
	}

	public ArrayList<SparseString> getProteinIDs() {
		return proteinIDs;
	}

	public void setProteinIDs(ArrayList<SparseString> proteinIDs) {
		this.proteinIDs = proteinIDs;
	}

}
