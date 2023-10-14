package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;



/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {

	private AggregateOperator operator;
	private boolean gBy;
	private TupleDesc td1;
	private ArrayList<Tuple> tupleList;
	private ArrayList<Tuple> groupByTupleList;
	private HashMap<String, Integer> groupbyHashMap;
	private HashMap<String, Integer> groupbyCount;
	private int max;
	private int min;
	private int currentSum;
	private int count;


	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		operator = o;
		gBy = groupBy;
		td1 = td;
		count = 0;
		tupleList = new ArrayList<Tuple>();
		groupByTupleList = new ArrayList<Tuple>();
		groupbyHashMap = new HashMap<>();
		groupbyCount = new HashMap<>();
		currentSum = 0;
		min = 999999999;
		max = 0;
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 * @throws Exception 
	 */
	public void merge(Tuple t) throws Exception {
		//your code here
		if(!gBy) {
			switch(operator) {
			case MAX:
				int maxValue = Integer.valueOf(t.getField(0).toString());
				if(maxValue > max) {
					max = maxValue;
				}
				break;
			case MIN:
				int minValue = Integer.valueOf(t.getField(0).toString());
				if(minValue < min) {
					min = minValue;
				}
				break;
			case SUM:
			case COUNT:
			case AVG:
				int sumValue = Integer.valueOf(t.getField(0).toString());
				currentSum += sumValue;
				count++;
				break;
			default:
				throw new Exception("There is an unknown operator");
			}
		}						
		else{ //groupBy is true
			String columnValue = t.getField(0).toString();
			int intValue = Integer.valueOf(t.getField(1).toString());
			switch(operator) {

			case MAX:
				if(groupbyHashMap != null  && groupbyHashMap.containsKey(columnValue)) {
					int maxValue = groupbyHashMap.get(columnValue);
					if(intValue > maxValue) {
						groupbyHashMap.put(columnValue, max);
					}
				}
				else { //does not contain columnValue
					groupbyHashMap.put(columnValue, max);
				}
				break;
			case MIN:
				if(groupbyHashMap != null  && groupbyHashMap.containsKey(columnValue)) {
					int minValue = groupbyHashMap.get(columnValue);
					if(intValue < minValue) {
						groupbyHashMap.put(columnValue, min);
					}
				}
				else {
					groupbyHashMap.put(columnValue, min);
				}
				break;
			case SUM:
				if(groupbyHashMap != null  && groupbyHashMap.containsKey(columnValue)) {
					int sumValue = groupbyHashMap.get(columnValue);
					currentSum = sumValue + intValue;
					groupbyHashMap.put(columnValue, currentSum);
				}
				else {
					groupbyHashMap.put(columnValue, intValue);
				}
				break;
			case COUNT:
				if(groupbyHashMap != null  && groupbyHashMap.containsKey(columnValue)) {
					int currentCount = groupbyHashMap.get(columnValue);
					int newCount = currentCount + 1;
					groupbyHashMap.put(columnValue, newCount);
				}
				else {
					count = 1;
					groupbyHashMap.put(columnValue, count);
				}
				break;
			case AVG:
				if(groupbyHashMap != null  && groupbyHashMap.containsKey(columnValue)) {
					int currentCount = groupbyCount.get(columnValue);
					int newCount = currentCount + 1;
					groupbyCount.put(columnValue, newCount);
					int oldAvg = groupbyHashMap.get(columnValue);

					int numer = (oldAvg * currentCount) + intValue;
					int denom = newCount;
					int newAvg = numer/denom;
					groupbyHashMap.put(columnValue, newAvg);
				}
				else {
					groupbyCount.put(columnValue, 1);
					groupbyHashMap.put(columnValue, intValue);
				}
				break;
			default:
				throw new Exception("There is an unknown operator");

			}

		}
	}



	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 * @throws Exception 
	 */
	public ArrayList<Tuple> getResults() throws Exception {
		//your code here
		
		if(!gBy) {
			Type[] type = new Type[1];
			String[] name = new String[1];
			type[0] = Type.INT;
			name[0] = "result";
			
			TupleDesc resultTd = new TupleDesc(type, name);
			Tuple resultTuple = new Tuple(resultTd);
			switch(operator) {
			case MAX:
				resultTuple.setField(0, new IntField(max));
				break;
			case MIN:
				resultTuple.setField(0, new IntField(min));
				break;
			case SUM: 
				resultTuple.setField(0, new IntField(currentSum));
				break;
			case COUNT: 
				resultTuple.setField(0, new IntField(count));
				break;
			case AVG:
				resultTuple.setField(0, new IntField(currentSum/count));
				break;
			default:
				throw new Exception("There is an unknown operator");
			}
			tupleList.add(resultTuple);
		}
		else { //groupBy is true
			Type[] type1 = new Type[2];
			String[] name1 = new String[2];
			type1[0] = Type.STRING;
			type1[1] = Type.INT;
			name1[0] = "category";
			name1[1] = "result";
			TupleDesc resultTd1 = new TupleDesc(type1, name1);
			
					
			for (String columnValue : groupbyHashMap.keySet()) {
				int result = groupbyHashMap.get(columnValue);
//				/*
//				 * switch(operator) { case MAX: case MIN: case SUM: case COUNT: case AVG:
//				 * 
//				 * break; default: throw new Exception("There is an unknown operator"); }
//				 */
				Tuple resultTuple1 = new Tuple(resultTd1);
				resultTuple1.setField(0, new StringField(columnValue));
				resultTuple1.setField(1, new IntField(result));
				tupleList.add(resultTuple1);
			}

		}
		
			return tupleList;
		
		
	}

}
