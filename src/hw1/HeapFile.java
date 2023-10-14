package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.RandomAccessFile;
/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	private File file;
	private TupleDesc tupleDesc;
	private int id;
	
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.id = f.hashCode();
		this.file = f;
		this.tupleDesc = type;
	}
	
	public File getFile() {
		//your code here
		return this.file;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.tupleDesc;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id){
		//your code here
		byte[] lengthBytesToScan = new byte[PAGE_SIZE];
		try {
		RandomAccessFile raf = new RandomAccessFile(this.file, "r");
		raf.seek(PAGE_SIZE * id);
        raf.readFully(lengthBytesToScan);
        raf.close();
        //error right now is here when i make the heap page
        if(raf==null) {
			return null;
		}
        //System.out.println(id);
        //System.out.println(lengthBytesToScan);
        //System.out.println(this.getId());
        //System.out.println("is raf null?");
        //System.out.println(raf==null);
        //System.out.println("HERE");
        HeapPage temp = new HeapPage(id, lengthBytesToScan, this.getId());
        //System.out.println("WON'T REACH");
        return temp;
		}
		catch(Exception e){
			//System.out.println("CATCH");
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws Exception 
	 */
	public void writePage(HeapPage p) throws Exception {
		//your code here
		try {
			RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
			raf.seek(PAGE_SIZE * p.getId());
		    raf.write(p.getPageData());
		    raf.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new Exception("Failed to write file");
		}
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws Exception 
	 */
	public HeapPage addTuple(Tuple t) throws Exception{
		 for (int i = 0; i < getNumPages(); i ++) {
	        	HeapPage readPage = readPage(i);
	        	for (int j = 0; j < readPage.getNumSlots(); j ++) {
	        		if(readPage.slotOccupied(j) == true) {
	        			//do nothing
	        		}
	        		else {
	                	readPage.addTuple(t);
	                	try {
		                    byte[] bytesToRead = readPage.getPageData();
		                    RandomAccessFile raf = new RandomAccessFile(file, "rw");
		                    raf.seek(PAGE_SIZE*i);
		                    raf.write(bytesToRead);
		                    raf.close();
		                }
		                catch (Exception e) {
		                    throw new Exception("Error in writing new page");
		                }
	                	return readPage;
	                }
	            }
	        }
		 	byte[] new_data = new byte[PAGE_SIZE];
	        HeapPage newPage = new HeapPage(this.getNumPages(), new_data , this.getId());
	        newPage.addTuple(t);
	        writePage(newPage);
	        return newPage;
		}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		this.id = t.getPid();
		for (Tuple tuple: getAllTuples()) {
			if (tuple.getPid() == this.id) {
				HeapPage readPage = readPage(tuple.getPid());
				try {
					readPage.deleteTuple(tuple);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					writePage(readPage);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return;
			}
		}
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
		for(int i=0; i< this.getNumPages(); i++) {
			Iterator<Tuple> it= this.readPage(i).iterator();
			Iterable<Tuple> iterable = () -> it;
			//System.out.println("HERE IN GET ALL");
			for(Tuple tuple: iterable) {
				tupleList.add(tuple);
			}
		}
		return tupleList;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		return (int)Math.ceil(file.length()/PAGE_SIZE);
	}
}
