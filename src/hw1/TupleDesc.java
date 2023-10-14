package hw1;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	this.types = typeAr;
    	this.fields = fieldAr;
    	
    }

  
	/**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        //your code here
    	return (this.fields).length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	try {
    		String field = this.fields[i];
    		return field;
    	}
    	catch(ArrayIndexOutOfBoundsException e) {
    		throw new NoSuchElementException("i is not a valid field reference");
		}
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
    	name = name.trim();
    	for(int i = 0; i < fields.length; i++)
    		try {
    			if (this.fields[i].equals(name)) {
        			return i;
        		}
    		}
        	catch(ArrayIndexOutOfBoundsException e) {
        		throw new NoSuchElementException("no field with matching name is found");
    		}
    	throw new NoSuchElementException("no field with matching name is found");
    	}

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
    	try {
    		Type type = this.types[i];
    		return type;
    		}
    	catch(ArrayIndexOutOfBoundsException e){
    		throw new NoSuchElementException("i is not a valid field reference");
    	}
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int total = 0;
    	for(int i=0; i<this.numFields(); i++) {
    		/**
    		 * Look at type doc and might be something like TYPE.int
    		 */
    		if(this.getType(i) == Type.INT){
    			total = total + 4;
    		}
    		else {
    			//check to see if this should be 129 (one byte holds length info)
    			total = total + 129;
    		}
    	}
    	return total;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	//your code here
    	//check 247 reference studio 7
    	
    	TupleDesc comparison = (TupleDesc) o;
    	if (comparison.getSize() == this.getSize()){
    		for(int i=0; i<this.numFields(); i++) {
    			if(!this.getType(i).equals(comparison.getType(i))) {
    				return false;
    			}
    		}
    		return true;
    	}
    	return false;
    }
    

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("Hashcode is ignored");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        //your code here
    	String[] fields = this.fields;
    	Type[] types = this.types;
    	String return_val = "";
    	String type_string = "";
    	//System.out.println(this.getType(2));
    	for(int i=0; i<this.numFields(); i++) {
    		//System.out.println(this.numFields());
    		if(types[i]== Type.INT){
    			type_string= "INT";
    			//System.out.println("INT");
    		}
    		else {
    			type_string = "STRING";
    			//System.out.println("STRING");
    		}
    		//System.out.println("HERE SECOND");
    		String temp = type_string + "(" + (String)fields[i];
    		if(i == this.numFields()-1) {
    			temp += ")";
    		}
    		else {
    			temp += "), ";
    		}
    		return_val+=temp;		
    	}
    	return return_val;
    }
    public static void main (String args[]) {
    	Type[] type = {Type.INT, Type.STRING, Type.INT};
    	String[] field  = {"height", "name", "weight"};
    	TupleDesc tupledesc = new TupleDesc(type, field);
    	System.out.println(tupledesc.toString());
    }    
}

    
