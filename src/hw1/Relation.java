package hw1;

import java.util.ArrayList;
import hw1.Aggregator;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td1) {
		//your code here
		tuples= l;	
		td = td1;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		
		ArrayList<Tuple> selectedTuples = new ArrayList<>();
		
	    // Loop through each tuple in the current relation
	    for (Tuple tuple : tuples) {
	        // Get the field value from the tuple based on the specified field number
	        Field fieldValue = tuple.getField(field);

	        // Compare the fieldValue with the operand using the given operator
	        boolean conditionMet = fieldValue.compare(op, operand);
	      
	        // If the condition is met, add the tuple to the selectedTuples list
	        if (conditionMet) {
	            selectedTuples.add(tuple);
	        }
	    }
	    
		/*
		ArrayList<Tuple> output = new ArrayList<>();
		for(Tuple t: this.tuples) {
			if(t.getField(field).compare(op, operand)) {
				output.add(t);
			}
		}
		this.tuples = output;
		return this;
		*/
	    //this.tuples = selectedTuples;
	    //return this;
	    // Create a new TupleDesc for the selected tuples (same as the original)
	    TupleDesc selectedTd = td;

	    // Create a new Relation using the selected tuples and the updated TupleDesc
	    Relation selectedRelation = new Relation(selectedTuples, selectedTd);

	    return selectedRelation;
	    
		
	//	return null;
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
	    // Create a copy of the original TupleDesc
		
	    //TupleDesc renamedTd = td;
	    
		//Tuple t =
	    // Loop through each field index and corresponding new name
		Type[] type = new Type[td.numFields()];
	    String[] nameList = new String[td.numFields()];
	    
	    for (int i = 0; i < td.numFields(); i++) {
	    	for(int j = 0; j < fields.size(); j++) {
	    		type[i] = td.getType(i);
	    		String name = td.getFieldName(i);
	    		if(td.nameToId(name)== fields.get(j)) {
	    			nameList[i] = names.get(j);
		    	}
	    		else {
	    			nameList[i] = td.getFieldName(i);
	    		}
	    	}
	    }
	    TupleDesc renamedTd = new TupleDesc(type, nameList);
	    
	    Relation renamedRelation = new Relation(tuples, renamedTd);
	    return renamedRelation;
	}

	public Relation project(ArrayList<Integer> fields) {
	    // Create a new TupleDesc for the projected relation
	    //Initialize an empty list to store the projected tuples
		
		ArrayList<Tuple> projectedTuples = new ArrayList<Tuple>();
		//TupleDesc projectedTd = new TupleDesc(null, null);
		Type[] type = new Type[fields.size()];
	    String[] name = new String[fields.size()];
	    int index = 0;
	    
	    // Add only the specified fields to the new TupleDesc
	    for (int field : fields) {
	    	//field = td.getFieldname(field)
	    	//type = td.getType(field)
	    	type[index] = td.getType(field);
	    	name[index] = td.getFieldName(field);	
	    	index++;
	    }
	    TupleDesc projectedTd = new TupleDesc(type, name);

	    
	    for(Tuple t: tuples) {
	    	Tuple tup = new Tuple(projectedTd);
	    	index = 0;
	    	for(int field : fields) {
	    		//
	    		if(td.getType(field) == Type.INT) {
	    			int num = Integer.valueOf(t.getField(field).toString());
	    			tup.setField(index,new IntField(num));
	    		}
	    		else if(td.getType(field) == Type.STRING) {
	    			String str = t.getField(field).toString();
	    			tup.setField(index, new StringField(str));
	    		}
	    		index++;
	    	}
	 	    projectedTuples.add(tup);
	    }
	    
	    //tuple.getField. 
	    Relation projectedRelation = new Relation(projectedTuples, projectedTd);
	    
	    return projectedRelation;
	}

	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		ArrayList<Tuple> joinedTuples = new ArrayList<>();
		
		TupleDesc td1 = this.td;
		TupleDesc td2 = other.td;
		
		
		int len1 = td1.numFields();
		int len2 = td2.numFields();
		//do it for types first
		Type[] mergedTypes = new Type[len1 + len2];
		//do it for names next.
		String[] mergedNames = new String[len1 + len2];
		
		for (int i = 0; i < len1; i++) {
		   mergedTypes[i] = td1.getType(i);
		   mergedNames[i] = td1.getFieldName(i);
		}
		for (int i = 0; i < len2; i++) {
		   mergedTypes[len1 + i] = td2.getType(i);
		   mergedNames[len1 + i] = td2.getFieldName(i);
		}
		TupleDesc joinedTd = new TupleDesc(mergedTypes, mergedNames); 
		 // Create a new relation with the merged types and field names
	    //Relation mergedRelation = new Relation(tuples, joinedTd);

	    // Now, iterate through the tuples of both relations and perform the join
	    for (Tuple tuple1 : this.getTuples()) {
	        for (Tuple tuple2 : other.getTuples()) {
	            // Compare the specified fields to check for a match
	            if (tuple1.getField(field1).equals(tuple2.getField(field2))) {
	                // Create a new tuple by merging the fields of both tuples
	                Tuple mergedTuple = new Tuple(joinedTd);
	                
	                for (int i = 0; i < len1; i++) {
	                    mergedTuple.setField(i, tuple1.getField(i));
	                }
	                for (int i = 0; i < len2; i++) {
	                    mergedTuple.setField(len1 + i, tuple2.getField(i));
	                }
	                
	                // Add the merged tuple to the result relation
	                joinedTuples.add(mergedTuple);
	            }
	        }
	    }
	    Relation mergedRelation = new Relation(joinedTuples, joinedTd);
	    return mergedRelation;
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 * @throws Exception 
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) throws Exception {
		//your code here
		Aggregator calculator = new Aggregator(op, groupBy, td);
		for(Tuple t: tuples) {
			calculator.merge(t);
		}
		ArrayList<Tuple> aggregateTuples = calculator.getResults();
		TupleDesc td1 = aggregateTuples.get(0).getDesc();
		
		Relation aggregateRelation = new Relation(aggregateTuples, td1);
		return aggregateRelation ;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		//ArrayList<Tuple> allTuples = new ArrayList<>();
	    //for (Tuple tuple : tuples) {
	      //  allTuples.add(tuple);
	    //}
	    return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		StringBuilder sb = new StringBuilder("");
		sb.append(td.toString());
		sb.append(" ");
		for(Tuple t: tuples) {
			sb.append(t.toString());
			sb.append(" ");
		}
		return sb.toString();
		
	}
	  /*
    switch (op) {
        case EQ:
            conditionMet = fieldValue.equals(operand);
            break;
        case NOTEQ:
            conditionMet = !fieldValue.equals(operand);
            break;
        case GT:
        	if(fieldValue.compare(op, operand)){
        		
        		conditionMet = true;
        	}
        case LT: 
        	if(fieldValue.compare(op, operand)){
        		conditionMet = true;
        	}
        case LTE: 
        	if(fieldValue.compare(op, operand)){
        		conditionMet = true;
        	}
        case GTE: 
        	if(fieldValue.compare(op, operand)){
        		conditionMet = true;
        	}
        	
       
    }
    */
	 public static void main (String args[]) {
		 
		 //ar.toString();
	 } 

}
