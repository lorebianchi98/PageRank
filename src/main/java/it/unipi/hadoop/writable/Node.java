package it.unipi.hadoop.writable;

import com.google.gson.Gson;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Node implements WritableComparable<Node> {
    private String title = null;
    private double pageRank;
    private List<String> adjacencyList = null;
    private boolean isNode;
    public Node() {
        adjacencyList = new ArrayList<String>();
    }

    public Node(double pageRank, List<String> adjacencyList, boolean isNode) {
        this.pageRank = pageRank;
        this.adjacencyList = adjacencyList;
        this.isNode = isNode;
    }

    public Node(String title, double pageRank, boolean isNode) {
        this.title = title;
        this.pageRank = pageRank;
        this.isNode = isNode;
    }

    public Node(String title, double pageRank, List<String> adjacencyList, boolean isNode) {
        this.title = title;
        this.pageRank = pageRank;
        this.adjacencyList = adjacencyList;
        this.isNode = isNode;
    }


    public void setFromJson(final String json) {
        Node node = new Gson().fromJson(json, Node.class);
        setFromNode(node);
    }

    public void setFromNode(Node node){
        setPageRank(node.getPageRank());
        setAdjacencyList(node.getAdjacencyList());
        setTitle(node.getTitle());
        setIsNode(node.isNode());
    }

    public double getPageRank(){
        return pageRank;
    }

    public void setPageRank(double pageRank){
        this.pageRank = pageRank;
    }

    public List<String> getAdjacencyList(){
        return adjacencyList;
    }

    public void setAdjacencyList(List<String> adjacencyList) {
        this.adjacencyList = adjacencyList;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(pageRank);

        //the size is written in order to know how many nodes need to be read
        out.writeInt(this.adjacencyList.size());
        for (String node: this.adjacencyList) {
            out.writeUTF(node);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.pageRank = in.readDouble();

        int size = in.readInt();
        this.adjacencyList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            this.adjacencyList.add(in.readUTF());
        }
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }


    public boolean isNode() {
        return isNode;
    }

    public void setIsNode(boolean isNode) {
        isNode = isNode;
    }

    @Override
    public int compareTo(Node node) {
        if (this.pageRank < node.getPageRank())
            return 1;
        else if (this.pageRank == node.getPageRank())
            return this.title.compareTo(node.getTitle());
        return -1;
    }

    public static void main(String[] args){
        Node n1 = new Node("diahane", 1.0d, null, true);
        Node n2 = new Node("diahane", 0.1d, null, true);
        Node n3 = new Node("cds", 2.0d, null, true);
        Node n4 = new Node("daiahane", 1.0d, null, true);

        System.out.println(n1.compareTo(n2));
    }

}
