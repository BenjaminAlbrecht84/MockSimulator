package util.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;

public class MyGraph {
	
	private ArrayList<MyNode> nodes = new ArrayList<MyNode>();
	
	public MyEdge newEdge(MyNode source, MyNode target) {
		MyEdge e = new MyEdge(this, source, target);
		source.addOutEdge(e);
		target.addInEdge(e);
		return e;
	}

	public MyNode newNode(MyNode v) {
		MyNode newNode = new MyNode(this, v.getLabel());
		newNode.setInfo(v.getInfo());
		nodes.add(newNode);
		return newNode;
	}

	public MyNode newNode() {
		MyNode newNode = new MyNode(this, null);
		nodes.add(newNode);
		return newNode;
	}
	
	public void deleteEdge(MyEdge e) {
		if (e.getOwner().equals(this)) {
			e.getSource().removeOutEdge(e);
			e.getTarget().removeInEdge(e);
		} else
			throw new RuntimeException("Wrong Owner Exception");
	}

	public void deleteNode(MyNode v) {
		if (v.getOwner().equals(this)) {
			Iterator<MyEdge> it = v.getInEdges();
			while (it.hasNext()) {
				MyEdge e = it.next();
				e.getSource().removeOutEdge(e);
			}
			it = v.getOutEdges();
			while (it.hasNext()) {
				MyEdge e = it.next();
				e.getTarget().removeInEdge(e);
			}
			nodes.remove(v);
		} else
			throw new RuntimeException("Wrong Owner Exception");
	}
	
	public void setLabel(MyEdge e, String label) {
		if (e.getOwner().equals(this))
			e.setLabel(label);
		else
			throw new RuntimeException("Wrong Owner Exception");
	}

	public void setLabel(MyNode v, String label) {
		if (v.getOwner().equals(this))
			v.setLabel(label);
		else
			throw new RuntimeException("Wrong Owner Exception");
	}

	public boolean isSpecial(MyEdge e) {
		if (e.getOwner().equals(this)) {
			if (e.getTarget().getInDegree() > 1)
				return true;
			return false;
		} else
			throw new RuntimeException("Wrong Owner Exception");
	}
	
	public void setSpecial(MyEdge e, boolean b) {
		if (e.getOwner().equals(this)) 
			e.setSpecial(b);
		else
			throw new RuntimeException("Wrong Owner Exception");
	}
	
	public void setInfo(MyEdge e, Object info) {
		if (e.getOwner().equals(this))
			e.setInfo(info);
		else
			throw new RuntimeException("Wrong Owner Exception");
	}

	public void setInfo(MyNode v, Object info) {
		if (v.getOwner().equals(this))
			v.setInfo(info);
		else
			throw new RuntimeException("Wrong Owner Exception");
	}
	
	public int getNumberOfNodes(){
		return nodes.size();
	}
	
	public ArrayList<MyNode> getNodes(){
		return nodes;
	}
}
