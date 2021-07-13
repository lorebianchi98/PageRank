package it.unipi.hadoop.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Node implements Writable {
    private String title = null;
    private double pageRank;
    private List<String> adjacencyList = null;

    public Node() {
        adjacencyList = new ArrayList<String>();
    }

    public Node(double pageRank, List<String> adjacencyList) {
        this.pageRank = pageRank;
        this.adjacencyList = adjacencyList;
    }

    public Node(String title, double pageRank) {
        this.title = title;
        this.pageRank = pageRank;
    }

    public Node(String title, double pageRank, List<String> adjacencyList) {
        this.title = title;
        this.pageRank = pageRank;
        this.adjacencyList = adjacencyList;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public void setAdjacencyList(List<String> adjacencyList) {
        this.adjacencyList = adjacencyList;
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

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
