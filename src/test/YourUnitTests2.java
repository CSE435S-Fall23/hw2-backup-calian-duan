package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.IntField;
import hw1.Query;
import hw1.Relation;



public class YourUnitTests2 { 
	
	private Catalog c;
	@Before
	public void setup() {
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
	}
	
	@Test
	public void testComplicateAlias() {
		Query q = new Query("SELECT a1, SUM(a2) as sum FROM A GROUP BY a1");
		Relation r = q.execute();
		
		assertTrue(r.getTuples().size() == 4);
	}
	
	@Test 
	public void testMax() {
		Query q = new Query("SELECT max(a1) FROM A");
		Relation r = q.execute();
		assertTrue(r.getTuples().size() == 1);
		IntField agg = (IntField) (r.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 530);
	}
	
}
