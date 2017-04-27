package util.graph;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.ArrayList;

import util.PhylipParser;

public class MyPhyloTree extends MyGraph {
	
	private String info;
	private String name;
	private MyNode root;

	public MyPhyloTree() {
		root = new MyNode(this, null);
	}

	public MyPhyloTree(MyNode root) {
		this.root = root;
		root.setOwner(this);
	}
	
	public static MyPhyloTree copy(MyPhyloTree t) {
		return new MyPhyloTree(t);
	}
	
	public MyPhyloTree(MyPhyloTree t) {
		MyNode v = t.getRoot();
		root = new MyNode(this, v.getLabel());
		root.setLabel(v.getLabel());
		copyTreeRec(t, v, root, new Hashtable<MyNode, MyNode>());
	}

	private void copyTreeRec(MyPhyloTree t, MyNode v, MyNode vCopy, Hashtable<MyNode, MyNode> visited) {
		visited.put(v, vCopy);
		Iterator<MyEdge> it = v.getOutEdges();
		while (it.hasNext()) {
			MyEdge e = it.next();
			MyNode c = e.getTarget();
			MyEdge eCopy;
			if (visited.containsKey(c)) {
				MyNode cCopy = visited.get(c);
				eCopy = newEdge(vCopy, cCopy);
			} else {
				MyNode cCopy = newNode(c);
				eCopy = newEdge(vCopy, cCopy);
				if (c.getOutDegree() != 0)
					copyTreeRec(t, c, cCopy, visited);
			}
			eCopy.setInfo(e.getInfo());
			eCopy.setLabel(e.getLabel());
		}
	}

	public Iterator<MyEdge> edgeIterator() {
		return getEdges().iterator();
	}

	public Iterator<MyNode> nodeIterator() {
		return getNodes().iterator();
	}

	public ArrayList<MyNode> getNodes() {
		ArrayList<MyNode> nodes = new ArrayList<MyNode>();
		getNodesRec(root, nodes);
		return nodes;
	}

	private void getNodesRec(MyNode v, ArrayList<MyNode> nodes) {
		if (!nodes.contains(v)) {
			nodes.add(v);
			Iterator<MyEdge> it = v.getOutEdges();
			while (it.hasNext())
				getNodesRec(it.next().getTarget(), nodes);
		}
	}

	public int getNumberOfNodes() {
		return getNodes().size();
	}

	public ArrayList<MyNode> getLeaves() {
		ArrayList<MyNode> nodes = getNodes();
		ArrayList<MyNode> leaves = new ArrayList<MyNode>();
		for (MyNode v : nodes) {
			if (v.getOutDegree() == 0)
				leaves.add(v);
		}
		return leaves;
	}

	public int getNumberOfLeaves() {
		return getLeaves().size();
	}

	public int getInDegree(MyNode v) {
		return v.getInDegree();
	}

	public int getOutDegree(MyNode v) {
		return v.getOutDegree();
	}

	public MyNode getSource(MyEdge e) {
		return e.getSource();
	}

	public MyNode getTarget(MyEdge e) {
		return e.getTarget();
	}

	public int getDegree(MyNode v) {
		return v.getInDegree() + v.getOutDegree();
	}

	public String getLabel(MyNode v) {
		return v.getLabel();
	}

	public String getLabel(MyEdge e) {
		return e.getLabel();
	}

	public ArrayList<MyEdge> getEdges() {
		ArrayList<MyEdge> edges = new ArrayList<MyEdge>();
		getEdgesRec(root, edges);
		return edges;
	}

	private void getEdgesRec(MyNode v, ArrayList<MyEdge> edges) {
		Iterator<MyEdge> it = v.getOutEdges();
		while (it.hasNext()) {
			MyEdge e = it.next();
			if (!edges.contains(e)) {
				edges.add(e);
				getEdgesRec(e.getTarget(), edges);
			}
		}
	}

	public Iterator<MyEdge> getInEdges(MyNode v) {
		return v.getInEdges();
	}

	public Iterator<MyEdge> getOutEdges(MyNode v) {
		return v.getOutEdges();
	}

	public int getNumberOfEdges() {
		return getEdges().size();
	}

	public MyNode getRoot() {
		return root;
	}

	public void setRoot(MyNode v) {
		root = v;
	}

	public String toMyBracketString() {
		return root.toMyNewick("", new ArrayList<MyNode>(), new Hashtable<MyNode, String>(), null) + ";";
	}

	public String toBracketString() {
		return root.toNewick("", new ArrayList<MyNode>(), new Hashtable<MyNode, String>()) + ";";
	}

	public String toString() {
		return root.toNewick("", new ArrayList<MyNode>(), new Hashtable<MyNode, String>());
	}
	
	public void parseBracketNotation(String newickString) {
		MyPhyloTree t = new PhylipParser().run(newickString);
		root = t.getRoot();
	}

	public void setWeight(MyEdge e, double d) {
		e.setWeight(d);
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public boolean isTree() {
		for (MyNode v : getNodes()) {
			if (v.getInDegree() > 1)
				return false;
		}
		return true;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

}
