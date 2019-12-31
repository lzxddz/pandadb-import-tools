package cn.pandadb.batchimport;

import com.sun.javafx.binding.StringFormatter;
import org.neo4j.graphdb.*;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.FileWriter;
import java.util.Map;

public class ImportNodeToSolr {

    CloudSolrClient solrClient;

    String solrIdName = "id";
    String solrLabelName = "labels";
    String solrPropPrefix = "";

    long countPerSolrCommit = 1000;  // solr一次写入间隔节点数
    long countPerLog = 100000;  // 日志一次写入间隔节点数

    public ImportNodeToSolr(String zkUri, String collectionName)
    {
        this.solrClient = new CloudSolrClient(zkUri);
        solrClient.setZkClientTimeout(30000);
        solrClient.setZkConnectTimeout(50000);
        solrClient.setDefaultCollection(collectionName);
    }


    public void doImport(ResourceIterator<Node> nodes, FileWriter logFileWriter) throws Exception
    {
        long nodeCount = 0;
        while ( nodes.hasNext()) {
            Node n1 = nodes.next();

            SolrInputDocument nodeDoc =  new SolrInputDocument();
            nodeDoc.addField(solrIdName, n1.getId());
            Iterable<Label> labels =  n1.getLabels();
            String labelsStr = "";
            for (Label lbl: labels) {
                labelsStr += ","+ lbl.name();
            }
            if(labelsStr.length() > 0){
                labelsStr = labelsStr.substring(1);
            }
            nodeDoc.addField(solrLabelName, labelsStr);

            Map<String, Object> props =  n1.getAllProperties();
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                nodeDoc.addField(solrPropPrefix+entry.getKey(), entry.getValue());
            }

            solrClient.add(nodeDoc);
            nodeCount += 1;
            if(nodeCount % countPerSolrCommit == 0) {
                solrClient.commit();
            }
            if(nodeCount % countPerLog == 0) {
                logFileWriter.write(String.format("%d,%d\n", nodeCount, System.currentTimeMillis()) );
                logFileWriter.flush();
            }
        }
        solrClient.commit();
        if(nodeCount % countPerLog != 0) {
            logFileWriter.write(String.format("%d,%d\n", nodeCount, System.currentTimeMillis()) );
            logFileWriter.flush();
        }
    }

}
