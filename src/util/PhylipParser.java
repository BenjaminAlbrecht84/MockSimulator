package util;

import java.util.Hashtable;
import java.util.Vector;

import util.graph.MyEdge;
import util.graph.MyNode;
import util.graph.MyPhyloTree;

public class PhylipParser {

	private Hashtable<Integer, Integer> bracketToBracket;
	private String newickString;

	public PhylipParser() {
		bracketToBracket = new Hashtable<Integer, Integer>();
	}

	public MyPhyloTree run(String newickString) {

		this.newickString = newickString;
		mapBrackets(newickString);

		MyPhyloTree t = new MyPhyloTree();
		int pos = newickString.length() - 1;
		String rootLabel = "";
		while (pos >= 0 && newickString.charAt(pos) != ')') {
			char c = newickString.charAt(pos);
			if (c != ';')
				rootLabel = c + rootLabel;
			pos--;
		}
		t.getRoot().setLabel(rootLabel);
		if (pos > 0)
			parseChildren(t, t.getRoot(), pos);

		return t;

	}

	private void mapBrackets(String newickString) {
		Vector<Integer> openBrackets = new Vector<Integer>();
		boolean insideLabel = false;
		for (int i = 0; i < newickString.length(); i++) {

			char c = newickString.charAt(i);

			if (c == '\'')
				insideLabel = !insideLabel;

			else if (!insideLabel) {
				if (c == '(')
					openBrackets.add(i);
				else if (c == ')') {
					int openBracket = openBrackets.lastElement();
					bracketToBracket.put(openBracket, i);
					bracketToBracket.put(i, openBracket);
					openBrackets.removeElement(openBracket);
				}
			}

		}
	}

	private void parseChildren(MyPhyloTree t, MyNode v, int i) {
		Hashtable<String, Vector<Integer>> labelToStartpos = splitBracket(i);
		for (String s : labelToStartpos.keySet()) {
			for (int startPos : labelToStartpos.get(s)) {
				String[] a = parseLabel(s);
				MyNode w = t.newNode();
				w.setLabel(a[0]);
				MyEdge e = t.newEdge(v, w);
				if (a[2] != null)
					e.setWeight(Double.parseDouble(a[2]));
				if (a[3] != null)
					e.setInfo(a[3].substring(0, a[3].length() - 1));
				if (startPos != -1)
					parseChildren(t, w, startPos);
			}
		}
	}

	private Hashtable<String, Vector<Integer>> splitBracket(int pos) {
		Hashtable<String, Vector<Integer>> labelToStartpos = new Hashtable<String, Vector<Integer>>();
		int i = pos - 1;
		String s = "";
		boolean insideLabel = false;
		while (i >= bracketToBracket.get(pos)) {
			char c = newickString.charAt(i);
			if (c == '\'')
				insideLabel = !insideLabel;
			if (!insideLabel) {
				if (c == ',' || c == '(') {
					String label = s;
					if (!labelToStartpos.containsKey(label))
						labelToStartpos.put(label, new Vector<Integer>());
					labelToStartpos.get(label).add(-1);
					s = "";
				} else if (c == ')') {
					String label = s;
					if (!labelToStartpos.containsKey(label))
						labelToStartpos.put(label, new Vector<Integer>());
					labelToStartpos.get(label).add(i);
					i = bracketToBracket.get(i) - 1;
					s = "";
				} else
					s = String.valueOf(c) + s;
			} else
				s = String.valueOf(c) + s;
			i--;
		}
		return labelToStartpos;
	}

	public String[] parseLabel(String l) {
		String[] content = new String[4];
		content[0] = l.substring(0, l.length() - 2).replaceAll("\\'", "");
		return content;
	}

}
