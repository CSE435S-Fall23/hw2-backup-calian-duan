package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;

public class Query {


	private String q;

	public Query(String q) {
		this.q = q;
	}
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}

		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		Relation returnR = null;
		boolean haveGroupBy = false;
		Catalog c = Database.getCatalog();
		FromItem fromItem = sb.getFromItem();
		String tableName = fromItem.toString();
		Object[] list = sb.getSelectItems().toArray();
		ArrayList<String> fieldNames= new ArrayList<String>();
		for(Object o:list) {
			String str = o.toString();
			fieldNames.add(str);  
		}

		int tableId = c.getTableId(tableName);
		TupleDesc td = c.getTupleDesc(tableId);
		HeapFile hf = c.getDbFile(tableId);

		ArrayList<Tuple> Tuples = hf.getAllTuples();
		Relation r = new Relation(Tuples, td);

		WhereExpressionVisitor wv = new  WhereExpressionVisitor(); 

		try{
			sb.getWhere().accept(wv);
			String left = wv.getLeft();
			Integer field =td.nameToId(left);
			Field right = wv.getRight();
			RelationalOperator op = wv.getOp();

			r = r.select(field, op, right);
		}
		catch(Exception e){

		}

		List<SelectItem> items = sb.getSelectItems();
		ArrayList<Tuple> aggregatorList = new ArrayList<Tuple>();
		//TupleDesc aggTd = new TupleDesc();
		if(items != null) { 
			ArrayList<Integer> gByFields = new ArrayList<Integer>();
			AggregateOperator ao= null;
			
			for(SelectItem i : items) {
				
				ColumnVisitor cv = new ColumnVisitor();
				i.accept(cv);
				boolean groupBy = sb.getGroupByColumnReferences() != null;
				haveGroupBy = groupBy;
				
				if(cv.isAggregate()) {
					ao  = cv.getOp();
					String column = cv.getColumn();
					
					//Type tp1 = td.getType(td.nameToId(column));
					//Type[] type1 = new Type[1];
				    //String[] nameList1 = new String[1];
				    //tp1[0] =
					if(!groupBy) {
						/*
						Type tp1 = td.getType(td.nameToId(column));
						Type[] type1 = new Type[1];
					    String[] column1 = new String[1];
					    type1[0] = tp1;
					    column1[0] = column;
					    TupleDesc aggTd = new TupleDesc(type1, column1);
					    Tuple aggTuple = new Tuple(aggTd);
					    Relation agg = new Relation(Tuples, aggTd);
					    */
					    ArrayList<Integer> aggFields = new ArrayList<Integer>();
					    aggFields.add(td.nameToId(column));
					    Relation projectedR= r.project(aggFields);
						
					    try {
							returnR = projectedR.aggregate(ao, groupBy);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}

				}
				else {
					if(haveGroupBy) {
						gByFields.add(td.nameToId(i.toString()));
					}
				}
			}
			
			if(haveGroupBy) {
				// now need to remap the fields to have groupBy fields is the first field
				Integer num = gByFields.get(0);
				for(int i = 0; i<td.numFields(); i++) {
					if(i !=num) {
						gByFields.add(i);
					}
				}
				r = r.project(gByFields);
				try {
					returnR = r.aggregate(ao, true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			//		//try {
			//		//	sb.getSelectItems().get(0).node; 
			//		//	if(selectItems ! = null) {
			//			//	for(SelectItem si : selectItems) {
			//			//		ColumnVisitor cv = new ColumnVisitor();
			//			//		si.accept(cv);
			//					//selectitem can accept columnVector 
			//					
			//					
			//			//		if(cv.isAggregate()) {
			//			//			AggregateOperator  = cv.getOp();
			//						
			//			//			boolean groupBy = sb.getGroupByColumnReferences() != null;
			//			//			String colName = cv.getColumn();
			//			//			if(colName.equals("*")) {
			//			//				getFieldname
			//			//			}
			//						
			//			//				if(!groupBy) {
			//			//					fields.add()
			//								//result.aggregate(op, false) //different ArrayList. 
			//							}
			//							(cv.getColumn){
			//								""
			//							}
			//							(nametoId(cv.getColumn))
			//						
			//						if(SelectExpressionItemsi).getAlias()!=null
			//								(ColumnList) alias.add((selectionExpressionItem)si.getAlias.getName) 
			//						
			//					}
			//				}
			//				
			//			}
			//			how to you extract the Expression. I don't understand how to call the certain functions in querry. 
			//			sb.
			//			sb.accept(sv);
			//			sv.visit(sb);
			//			av.isAggregate()
			//			av.
			//		}
			//		catch(Exception e){
			//			
			//		}
		}

			try {
				int joinTimes=  sb.getJoins().size();	
				for(int i = 0; i< joinTimes; i++) {
					Join join = sb.getJoins().get(i);
					String tb= join.getRightItem().toString(); // Get the name of the table being joined
					String joinCondition = join.getOnExpression().toString(); // Get the join condition
					ArrayList<String> fieldNames1= findFieldsinJoins(tableName, tb, joinCondition);

					// get tupleDesc and tuples from other
					int jointableId = c.getTableId(tb);
					HeapFile joinedHf = c.getDbFile(jointableId);

					ArrayList<Tuple> JoinTableTuples = joinedHf.getAllTuples();
					TupleDesc joinTd = c.getTupleDesc(jointableId);
					Relation other = new Relation(JoinTableTuples, joinTd);

					// the first is from the table field, the second is from join table field
					int id1 = td.nameToId(fieldNames1.get(0));
					int id2 = joinTd.nameToId(fieldNames1.get(1));
					r = r.join(other, id1, id2);
				}

			}
			catch(Exception e){

			}

			if(returnR == null) {
				TupleDesc newTd = r.getDesc();
	
				//find fieldIndexs
				ArrayList<Integer> fields = new ArrayList<Integer>();
				// if *, simply add all from tupleDesc
				if(fieldNames.get(0).trim().equals("*")) {
					for(int i = 0; i < newTd.numFields(); i++) {
						fields.add((Integer)i);
					}
				}
				else {
					for(String str:fieldNames) {
						Integer i =newTd.nameToId(str);
						fields.add(i);
					}
				}
	
				returnR = r.project(fields);
			}

			//your code here

			return returnR;

		}
		private ArrayList<String> findFieldsinJoins(String tableName, String tb, String joinCondition)
		{

			ArrayList<String> fieldNames = new ArrayList<String>();
			String[] parts= joinCondition.split("=");

			assert(parts.length== 2);
			String left = parts[0];
			String  right = parts[1];
			String[] leftParts= left.split("\\.");
			String[] rightParts= right.split("\\.");
			assert(leftParts.length== 2);
			assert(rightParts.length== 2);
			if(leftParts[0].toLowerCase().trim().equals(tableName.toLowerCase())){
				fieldNames.add(leftParts[1].trim());	
				assert(rightParts[0].toLowerCase().trim().equals(tb.toLowerCase()));
				fieldNames.add(rightParts[1].trim());	
			}
			else if(rightParts[0].toLowerCase().trim().equals(tableName.toLowerCase())){
				fieldNames.add(rightParts[1].trim());	
				assert(leftParts[0].toLowerCase().equals(tb.toLowerCase()));
				fieldNames.add(leftParts[1].trim());	

			}
			return fieldNames;

		}
	}
