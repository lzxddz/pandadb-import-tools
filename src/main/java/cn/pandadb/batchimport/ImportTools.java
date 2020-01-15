package cn.pandadb.batchimport;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import com.sun.tools.internal.xjc.reader.gbind.Graph;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;


public class ImportTools {

    public static String nowDate() {
        Date now = new Date();
        SimpleDateFormat dateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(now);
    }

    public static void importToSolr(Properties props, GraphDatabaseService db, FileWriter logFw)  throws Exception {
        String solrZkUri;
        String solrCollectionName;
        if (props.containsKey("external.properties.store.solr.zk")) {
            solrZkUri = props.get("external.properties.store.solr.zk").toString();
        }
        else {
            throw new Exception("Configure File Error: external.properties.store.solr.zk is not exist! ");
        }

        if (props.containsKey("external.properties.store.solr.collection")) {
            solrCollectionName = props.get("external.properties.store.solr.collection").toString();
        }
        else {
            throw new Exception("Configure File Error: external.properties.store.solr.collection is not exist! ");
        }

        Transaction tx = db.beginTx();
        ResourceIterator<Node> nodes = db.getAllNodes().iterator();
        ImportNodeToSolr solrImport = new ImportNodeToSolr(solrZkUri, solrCollectionName);
        solrImport.doImport(nodes, logFw);
        tx.close();
    }

    public static void importToEs(Properties props, GraphDatabaseService db, FileWriter logFw)  throws Exception {
        String esHost, indexName, typeName, esSchema;
        int esPort = 9200;

        if (props.containsKey("external.properties.store.es.host")) {
            esHost = props.get("external.properties.store.es.host").toString();
        }
        else {
            throw new Exception("Configure File Error: external.properties.store.es.host is not exist! ");
        }

        if (props.containsKey("external.properties.store.es.port")) {
            esPort = Integer.getInteger(props.get("external.properties.store.es.port").toString()) ;
        }
        else {
            throw new Exception("Configure File Error: external.properties.store.es.port is not exist! ");
        }

        if (props.containsKey("external.properties.store.es.schema")) {
            esSchema = props.get("external.properties.store.es.schema").toString();
        }
        else {
            throw new Exception("Configure File Error: external.properties.store.es.schema is not exist! ");
        }

        if (props.containsKey("external.properties.store.es.index")) {
            indexName = props.get("external.properties.store.es.index").toString();
        }
        else {
            throw new Exception("Configure File Error: external.properties.store.es.index is not exist! ");
        }

        if (props.containsKey("external.properties.store.es.type")) {
            typeName = props.get("external.properties.store.es.type").toString();
        }
        else {
            throw new Exception("Configure File Error: external.properties.store.es.type is not exist! ");
        }

        Transaction tx = db.beginTx();
        ResourceIterator<Node> nodes = db.getAllNodes().iterator();
        ImportNodeToEs esImport = new ImportNodeToEs(esHost, esPort, esSchema, indexName, typeName);
        esImport.doImport(nodes, logFw);
        tx.close();
    }

    public static void main(String[] args) throws Exception {

        String propFilePath = "/home/bigdata/pandadb-import-tools/testdata/pandadb-import-tools.conf"; // null;
        if (args.length > 0) {
            propFilePath = args[0];
        }

        Properties props  = new Properties();
        props.load(new FileInputStream(new File(propFilePath)));

        String importTo = "solr";
        String graphPath = null;
        String logFilePath;

        if (props.containsKey("import.to")) {
            importTo = props.get("import.to").toString();
        }
        else {
            throw new Exception("Configure File Error: import.to is not exist! ");
        }

        if (props.containsKey("neo4j.graph.path")) {
            graphPath = props.get("neo4j.graph.path").toString();
        }
        else {
            throw new Exception("Configure File Error: neo4j.graph.path is not exist! ");
        }

        if (props.containsKey("log.file.path")) {
            logFilePath = props.get("log.file.path").toString();
        }
        else {
            throw new Exception("Configure File Error: log.file.path is not exist! ");
        }

        File graphFile = new File(graphPath);
        if (!graphFile.exists()) {
            throw new Exception(String.format("Error: GraphPath(%s) is not exist! ", graphPath));
        }

        FileWriter logFw = new FileWriter(logFilePath);

        System.out.println("==== begin import ====");
        System.out.println(nowDate());
        System.out.println(props);

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(graphFile);

        switch (importTo) {
            case "solr":
                importToSolr(props, db, logFw);
                break;
            case "es":
                importToEs(props, db, logFw);
                break;
            default:
                System.out.println("invalid import to!");
        }

        logFw.flush();
        logFw.close();

        System.out.println("==== end import ====");
        System.out.println(nowDate());
//        //System.out.println("====end==="+maxCount+"nodes");
//        System.out.println("====end==="+nodeCount+"nodes");
//        String endTime = nowDate();
//        System.out.println("Begin Time: " +beginTime);
//        System.out.println("End Time: " +endTime);


    }
}
