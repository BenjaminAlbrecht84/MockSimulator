/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util.graphCaner.tree;

/**
 *
 * @author bagci
 */
class Edge {
    private Node target;

    public Edge() {
    }

    public Edge(Node target) {
        this.target = target;
    }

    public void setTarget(Node target) {
        this.target = target;
    }

    public Node getTarget() {
        return target;
    }
    
}
