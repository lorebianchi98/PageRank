package it.unipi.hadoop.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageParser {
    private String page;

    public PageParser(){

    }

    public void setPage(String page){
        this.page = page;
    }

    public String getTitle(){
        Pattern pattern = Pattern.compile("<title.*?>(.*?)</title>");
        Matcher matcher = pattern.matcher(page);

        if(matcher.find())
            return matcher.group(1); //remove /t to be able to use it as a separator
        else
            System.out.println("nop");
            return null;
    }

    public List<String> getOutLinks(){
        Pattern pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
        Matcher matcher = pattern.matcher(page);

        List<String> outLinks = new ArrayList<String>();
        while(matcher.find())
            outLinks.add(matcher.group(1));

        return outLinks;
    }
}
