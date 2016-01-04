package utils;

/**
 * Created by wangzehui on 12/28/15.
 */
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import nytparser.NYTCorpusDocument;
import nytparser.NYTCorpusDocumentParser;
import org.apache.pig.EvalFunc;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;


public class ProcessXML extends EvalFunc<Tuple> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
           // reporter.progress("PROGRESS...");

            String article = input.get(0).toString();

            return processXML(article);
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row", e);

        }
    }

    private Tuple processXML(String article) {

        Tuple outerTuple = mTupleFactory.newTuple();

        try {

            NYTCorpusDocumentParser parser = new NYTCorpusDocumentParser();
            NYTCorpusDocument doc = parser
                    .parseNYTCorpusDocumentFromDOMDocument(article);

            // add abstract
            String articleAbstract = doc.getArticleAbstract();
            outerTuple.append(checkNull(articleAbstract));


            //add lead paragraph
            String leadParagraph = doc.getLeadParagraph();
            outerTuple.append(checkNull(leadParagraph));

            URL url = doc.getUrl();
            outerTuple.append(checkNull(url));

            // list of titles
            List<String> titles = doc.getTitles();
            DataBag titlesDataBag = mBagFactory.newDefaultBag();
            for (String title : titles) {
                Tuple titleTuple = mTupleFactory.newTuple();
                titleTuple.append(title);
                titlesDataBag.add(titleTuple);
            }
            outerTuple.append(titlesDataBag);

            // list of online titles
            List<String> onlineTitles = doc.getOnlineTitles();
            DataBag onlineTitlesDataBag = mBagFactory.newDefaultBag();
            for (String onlineTitle : onlineTitles) {
                Tuple onlineTitleTuple = mTupleFactory.newTuple();
                onlineTitleTuple.append(onlineTitle);
                onlineTitlesDataBag.add(onlineTitleTuple);
            }
            outerTuple.append(onlineTitlesDataBag);


            HashMap<String, String> metaDataMap = new HashMap<String, String>();
            // meta data
            String dsk = doc.getNewsDesk();
            metaDataMap.put("dsk", dsk);

            URL alternateURL = doc.getAlternateURL();
            metaDataMap.put("alternateURL", checkNull(alternateURL));

            String onlineSection = doc.getOnlineSection();
            metaDataMap.put("onlineSection", checkNull(onlineSection));

            Integer printPageNumber = doc.getPage();
            metaDataMap.put("printPageNumber", checkNull(printPageNumber));

            String printSection = doc.getSection();
            metaDataMap.put("section", checkNull(printSection));

            String slug = doc.getSlug();
            metaDataMap.put("slug", checkNull(slug));

            Integer columnNumber = doc.getColumnNumber();
            metaDataMap.put("columnNumber", checkNull(columnNumber));

            String banner = doc.getBanner();
            metaDataMap.put("banner", checkNull(banner));

            Date correctionDate = doc.getCorrectionDate();
            metaDataMap.put("correctionDate", checkNull(correctionDate));

            String featurePage = doc.getFeaturePage();
            metaDataMap.put("featurePage", checkNull(featurePage));

            String columnName = doc.getColumnName();
            metaDataMap.put("columnName", checkNull(columnName));

            String seriesName = doc.getSeriesName();
            metaDataMap.put("seriesName", checkNull(seriesName));

            Integer dayOfMonth = doc.getPublicationDayOfMonth();
            metaDataMap.put("dayOfMonth", checkNull(dayOfMonth));

            Integer month = doc.getPublicationMonth();
            metaDataMap.put("month", checkNull(month));

            Integer year = doc.getPublicationYear();
            metaDataMap.put("year", checkNull(year));

            String dayOfWeek = doc.getDayOfWeek();
            metaDataMap.put("dayOfWeek", checkNull(dayOfWeek));

            outerTuple.append(metaDataMap);


            // add headline
            String headline = doc.getHeadline();
            outerTuple.append(headline);

            // add content
            String content = doc.getBody();
            outerTuple.append(content);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return outerTuple;

    }
    private String checkNull(Object obj){
        if(obj == null){
            return "";
        }else{
            return obj.toString();
        }
    }

    public Schema outputSchema(Schema input) {
        // Check that we were passed two fields
        if (input.size() != 1) {
            throw new RuntimeException(
                    "Expected (chararray), input does not have 1 fields");
        }

        try {
            // Get the types for both columns and check them. If they are
            // wrong, figure out what types were passed
            if (input.getField(0).type != DataType.CHARARRAY) {
                String msg = "Expected input (chararray, int), received schema (";
                msg += DataType.findTypeName(input.getField(0).type);
                msg += ")";
                throw new RuntimeException(msg);
            }

            Schema outerTuple = new Schema();

            // add abstract
            FieldSchema artAbstract = new Schema.FieldSchema("abstract",
                    DataType.CHARARRAY);
            outerTuple.add(artAbstract);

            // add lead paragrapg
            FieldSchema leadParagraph = new Schema.FieldSchema("leadParagraph",
                    DataType.CHARARRAY);
            outerTuple.add(leadParagraph);

            // add lead paragrapg
            FieldSchema url = new Schema.FieldSchema("url",
                    DataType.CHARARRAY);
            outerTuple.add(url);

            // adding titles
            Schema titles = new Schema();
            titles.add(new Schema.FieldSchema("title", DataType.CHARARRAY));
            FieldSchema titlesCol = new Schema.FieldSchema("titles",
                    titles, DataType.BAG);
            outerTuple.add(titlesCol);

            // adding online titles
            Schema onlineTitles = new Schema();
            onlineTitles.add(new Schema.FieldSchema("onlineTitle", DataType.CHARARRAY));
            FieldSchema onlineTitlesCol = new Schema.FieldSchema("onlineTitles",
                    onlineTitles, DataType.BAG);
            outerTuple.add(onlineTitlesCol);

            // adding metadata, consider map as scalar
            outerTuple.add(new Schema.FieldSchema("metaData", DataType.MAP));

            // adding headline
            FieldSchema headline = new Schema.FieldSchema("headline",
                    DataType.CHARARRAY);
            outerTuple.add(headline);

            // adding content
            FieldSchema content = new Schema.FieldSchema("content",
                    DataType.CHARARRAY);
            outerTuple.add(content);

            return new Schema(new Schema.FieldSchema(getSchemaName(this
                    .getClass().getName().toLowerCase(), input), outerTuple,
                    DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }
}
