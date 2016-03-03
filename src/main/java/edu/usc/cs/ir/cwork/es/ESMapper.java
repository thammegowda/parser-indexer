package edu.usc.cs.ir.cwork.es;

import edu.usc.cs.ir.cwork.solr.ContentBean;
import org.apache.nutch.indexer.NutchDocument;
import org.json.JSONObject;

/**
 * Created by tg on 3/3/16.
 */
public class ESMapper  {

    public static NutchDocument beanToNutchDoc(ContentBean contentBean) {
        NutchDocument doc = new NutchDocument();

        String id = contentBean.getId();
        doc.add("obj_id", id);
        /*FIXME: The below info not available
        if (datum.getMetaData().containsKey("obj_parent")) {
            doc.add("obj_parent", datum.getMetaData().get("obj_parent").toString());
        } else {
            if (inlinks != null && inlinks.size() > 0) {
                doc.add("obj_parent", DigestUtils.sha256Hex(inlinks.iterator().next().getFromUrl().toUpperCase()));
            } else {
                // do nothing
            }
        }
        */
        doc.add("obj_children", contentBean.getOutpaths());
        doc.add("obj_childrenurls", contentBean.getOutlinks());

        doc.add("content_type", contentBean.getContentType());
        //doc.add("crawl_data", extractedData);
        doc.add("crawler", "Nutch-1.12-SNAPSHOT");

        JSONObject extractedMd = new JSONObject(contentBean.getMetadata());
        doc.add("extracted_metadata", extractedMd);
        doc.add("extracted_text", contentBean.getContent());
        doc.add("obj_original_url", contentBean.getUrl());
        String storedUrl = id.replace(
                "file:/data2/USCWeaponsStatsGathering/nutch/full_dump/",
                "http://imagecat.dyndns.org/weapons/alldata/");
        doc.add("obj_stored_url", storedUrl);

        doc.add("team", "NASA_JPL");
        //FIXME: The below info not available
        // doc.add("timestamp", datum.getFetchTime());
        doc.add("url", contentBean.getUrl());
        doc.add("version", (float)2.0);

        doc.add("raw_content", contentBean.getRawContent());
        return doc;
    }

}
