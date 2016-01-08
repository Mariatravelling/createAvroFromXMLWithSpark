package com.dependencyParse;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class testSentence {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		SentencesLenConstraint a=new SentencesLenConstraint();
		TupleFactory mTupleFactory = TupleFactory.getInstance();
		Tuple tuple=mTupleFactory.newTuple("Few have tried, and far fewer have succeeded.");
		tuple.append(1);
		DataBag result=a.exec(tuple);
		Iterator<Tuple> iter = result.iterator();
		while (iter.hasNext())
	    {
	      List<Object> str=iter.next().getAll();
	      for(int j=0;j<str.size();j++)
	      {
	      String id=str.get(j).toString();
	      System.out.println("the "+j+id);

	      }
	    }
	}
}
