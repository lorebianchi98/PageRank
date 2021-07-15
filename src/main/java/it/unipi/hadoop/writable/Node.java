package it.unipi.hadoop.writable;

import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Node implements WritableComparable<Node> {
    private String title;
    private double pageRank;
    private List<String> adjacencyList;
    private boolean isNode;
    public Node() {
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
        out.writeUTF(title);
        out.writeDouble(pageRank);

        //the size is written in order to know how many nodes need to be read
        out.writeInt(this.adjacencyList.size());
        for (String node: this.adjacencyList) {
            out.writeUTF(node);
        }
        out.writeBoolean(isNode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.title = in.readUTF();
        this.pageRank = in.readDouble();

        int size = in.readInt();
        this.adjacencyList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            this.adjacencyList.add(in.readUTF());
        }
        this.isNode = in.readBoolean();
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }


    public boolean isNode() {
        return isNode;
    }

    public void setIsNode(boolean isNode) {
        this.isNode = isNode;
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
        String json = "{\"title\":\"07478\",\"pageRank\":4.1203131437989287E-4,\"adjacencyList\":[\"Bion 2\",\"Bion 2\"],\"isNode\":true}";
        Node node = new Node();
        node.setFromJson(json);
        Node node1 = new Node();
        node1.setFromNode(node);

        System.out.println(node1);
    }

}
