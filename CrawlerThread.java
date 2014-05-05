import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;


public class CrawlerThread extends Thread {
    
    private String _id;
    private int _iteration;
    private Context _context;

    public CrawlerThread(String id, int iteration, Context context) {
	_id = id;
	_iteration = iteration;
	_context = context;
    }
    
    public void run() {
	SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
	try {
	    if (_id.substring(0, 1).equals("P")) {
		System.out.println(" $$ " + sdf.format(new Date()) + " Crawling product " + _id.substring(1));                        
		crawlProduct(_id.substring(1));                                                     
	    }
	    if (_id.substring(0, 1).equals("U")) {
		System.out.println(" $$ " + sdf.format(new Date()) + " Crawling user " + _id.substring(1));                           
		crawlUser(_id.substring(1));                                                     
	    }
	} catch (StringIndexOutOfBoundsException e) {
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public void crawlUser(String id) throws IOException, InterruptedException {
	boolean loop = true;
	int pageNumber = 1;

	while (loop) {
	    String url = "http://www.amazon.com/gp/cdp/member-reviews/" + id + "?page=";
	    Document doc;
	    int i = 0;

	    while (true) {
		if (i > 15)
		    return;

		i++;

		try {
		    doc = Jsoup.connect(url + pageNumber).timeout(3000).get();
		    break;
		} catch (SocketTimeoutException e) {
		    System.out.println("..  Timeout, retrying");                                                        
		} catch (HttpStatusException e) {
		    if (e.getStatusCode() == 500) {
			System.out.println("..  Internal server error, retrying");                                      
		    } else {
			System.out.println("## " + e.toString());                                                       
			return;
		    }
		} catch (IOException e) {
		    System.out.println("## " + e.toString());                                                           
		    e.printStackTrace();
                                                                                                                              
		    return;
		}
	    }

	    Elements links = doc.select("a");
	    links = doc.select("td td tr td b a");

	    for (Element el : links) {
		String outer = el.outerHtml();
		if (outer.contains("/dp/")) {
		    try {
		    String productId = el.attr("href").substring(outer.indexOf("/dp/") - 5);

		    _context.write(new Text("P" + productId), new IntWritable(_iteration + 1));
		    } catch (StringIndexOutOfBoundsException e) {
			e.printStackTrace();
		    }
		}
	    }

	    pageNumber++;
	    if (doc.getElementsByAttributeValueMatching("href", "gp/cdp/member-reviews/" + id + ".*page=" + pageNumber + ".*").first() == null)
		loop = false;
	    //else                                                                                                        
	    if ((pageNumber % 10) == 0) {                                                                                 
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
		System.out.println(sdf.format(new Date()) + " ----- Crawling page: " + pageNumber);                                          
	    }
	}
    }

    public void crawlProduct(String id) throws IOException, InterruptedException {
	boolean loop = true;
	int pageNumber = 1;

	while (loop) {
	    loop = false;

	    String url = "http://www.amazon.com/product-reviews/" + id + "/ref=cm_cr_pr_top_link_41&pageNumber=";
	    Document doc;
	    int i = 0;

	    while (true) {
		if (i > 15)
		    return;
		
		i++;

		try {
		    doc = Jsoup.connect(url + pageNumber).timeout(3000).get();
		    break;
		} catch (SocketTimeoutException e) {
		    System.out.println(".. Timeout, retrying");                                                                                   
		} catch (HttpStatusException e) {
		    if (e.getStatusCode() == 500) {
			System.out.println(".. Internal server error, retrying");                                                                 
		    } else {
			System.out.println("## " + e.toString());                                                                                 
			return;
		    }
		} catch (IOException e) {
		    System.out.println("## " + e.toString());                                                                                     
		    return;
		}
	    }

	    Elements links = doc.select("a");
	    for (Element el : links) {
		String outer = el.outerHtml();
		if (outer.contains("/profile/")) {
		    try {
		    String userId = el.attr("href").substring(outer.indexOf("/profile/"));
		    _context.write(new Text("U" + userId), new IntWritable(_iteration + 1));
		    } catch (StringIndexOutOfBoundsException e) {
			e.printStackTrace();
		    }
		}
	    }

	    Element p = doc.select("span[class=paging]").first();
	    if (p != null) {
		Elements ps = p.select("a");
		for (Element el : ps) {
		    if (el.text().contains("Next")) {
			pageNumber++;
			loop = true;
		    }
		}
	    }

	    if ((pageNumber % 10) == 0) {
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
		System.out.println(sdf.format(new Date()) + " ----- Crawling page: " + pageNumber);
	    }
	}
    }
    
}
