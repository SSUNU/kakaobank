package ex4;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.lang.reflect.Field;
import java.nio.file.Files;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestHadoopBuilder {
	
	protected String hdfsUri;
	protected Configuration conf;
	private File testDir;
	private MiniDFSCluster miniDFSCluster;

	@BeforeAll
	public void setUp() throws Exception {
        
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
			File winutilsFile = new File(getClass().getClassLoader().getResource("hadoop/bin/winutils.exe").toURI());
			System.setProperty("hadoop.home.dir",winutilsFile.getParentFile().getParentFile().getAbsolutePath());
			System.setProperty("java.library.path", winutilsFile.getParentFile().getAbsolutePath());
			
			Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
			fieldSysPath.setAccessible(true);
			fieldSysPath.set(null, null);
		}
	
		testDir = Files.createTempDirectory("test_hdfs").toFile();
		System.out.println(testDir.getAbsolutePath());
		
		conf = new Configuration();
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDir.getAbsolutePath());
	
		miniDFSCluster = new MiniDFSCluster.Builder(conf).nameNodePort(11000).build();
	
		hdfsUri = miniDFSCluster.getURI().toString();
    }

    @AfterAll
    public void tearDown() {
        miniDFSCluster.shutdown();
		FileUtil.fullyDelete(testDir);
    }
    
    @Test
    public void test() throws Exception {
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	
    	try {
    		///////////Json Read
        	int concurrency = 0;
        	JSONObject sourceTable = null;
        	JSONObject targetTable = null;
    		
			File f = new File(getClass().getClassLoader().getResource("jobs.json").toURI()); 
			
			JSONParser jParser = new JSONParser();
			JSONArray jArr = (JSONArray)jParser.parse(new FileReader(f));
			
			for (Object item : jArr) {
				JSONObject jObj_root = (JSONObject)item;
				
				JSONObject jObj_child = (JSONObject)jObj_root.get("MenuLog_ETL_Job");
				concurrency = Integer.parseInt(jObj_child.get("concurrency").toString());
				sourceTable = (JSONObject)jObj_child.get("source_table");
				targetTable = (JSONObject)jObj_child.get("target_table");
			}

			
			///////////Target Prepare    	
	    	MessageType schema = MessageTypeParser.parseMessageType(
	    	          "message "+ targetTable.get("schema_name").toString() +" { "
	    	                  + "required binary LOG_TKTM; "
	    	                  + "required binary LOG_ID; "
	    	                  + "optional binary USR_NO; "
	    	                  + "optional binary MENU_NM; "
	    	                  + "} ");
	    	
	    	String outputFilePath = testDir + "/" + System.currentTimeMillis() + ".parquet";
	        File outputParquetFile = new File(outputFilePath);
	        Path path = new Path(outputParquetFile.toURI().toString()); //parquet path
	        
//	        GroupWriteSupport.setSchema(schema, conf);
//	        SimpleGroupFactory gf = new SimpleGroupFactory(schema);
//	        ParquetWriter<Group> writer = new ParquetWriter<Group>(
//	        		path,
//	        		new GroupWriteSupport(),
//	        		CompressionCodecName.SNAPPY,
//	        		ParquetWriter.DEFAULT_BLOCK_SIZE,
//	        		ParquetWriter.DEFAULT_PAGE_SIZE,
//	        		1048576,
//	        		true,
//	        		false,
//	        		ParquetProperties.WriterVersion.PARQUET_1_0,
//	        		conf);
	        	
	        ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
	                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
	                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
	                .withSchema(new AvroSchemaConverter().convert(schema))
	                .withConf(conf)
	                .withCompressionCodec(CompressionCodecName.SNAPPY)
	                .withValidation(false)
	                .withDictionaryEncoding(false)
	                .build();
			
	        
	        ///////////Source Data Get
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			if("".equals(sourceTable.get("gijun_date").toString())) {
				//Now()-1day
	    		cal.add(Calendar.DATE,-1); //-1���� ������ �̵�
			}
			else {
				//gijun_date - 1day
				cal.setTime(sdf.parse(sourceTable.get("gijun_date").toString()));
				cal.add(Calendar.DATE,-1);
			}
			
			String gijunStartDT = sdf.format(cal.getTime()) + "000000"; //���۽ð�
			String gijunEndDT = sdf.format(cal.getTime()) + "235959"; //�������ð�

    		//MySql
    		String url = "jdbc:mysql://localhost:3306/kakaobank?characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false";
    		String user = "root";
    		String password = "Adminsun123!@#";
    		
    		StringBuilder query = new StringBuilder();
    		query.append("SELECT LOG_TKTM, LOG_ID, USR_NO, MENU_NM ");
    		query.append("FROM " + sourceTable.get("table_name").toString() + " ");
    		query.append("WHERE LOG_TKTM BETWEEN '"+ gijunStartDT +"' AND '" + gijunEndDT+"' ");
    		query.append(";");
    		
    		System.out.println("query: " + query.toString());
    		
    		conn = DriverManager.getConnection(url,user,password);
    		stmt = conn.createStatement();
    		rs = stmt.executeQuery(query.toString());
    		
    		///////////Target Data Set
//    		while (rs.next()) {
//    			writer.write(gf.newGroup().append("LOG_TKTM", rs.getString("LOG_TKTM")));
//    			writer.write(gf.newGroup().append("LOG_ID", rs.getString("LOG_ID")));
//    			writer.write(gf.newGroup().append("USR_NO", rs.getString("USR_NO")));
//    			writer.write(gf.newGroup().append("MENU_NM", rs.getString("MENU_NM")));
//			}		
    		
    		while (rs.next()) {
    			GenericData.Record record = new GenericData.Record(new AvroSchemaConverter().convert(schema));
    			
    			record.put("LOG_TKTM", rs.getString("LOG_TKTM").getBytes());
    			record.put("LOG_ID", rs.getString("LOG_ID").getBytes());
    			record.put("USR_NO", rs.getString("USR_NO").getBytes());
    			record.put("MENU_NM", rs.getString("MENU_NM").getBytes());
    			
    			writer.write(record);
			}
    		
    		if(writer != null) {
				writer.close();
			}
    		
    		///////////Source Data Delete
    		query.setLength(0);
    		query.append("DELETE ");
    		query.append("FROM " + sourceTable.get("table_name").toString() + " ");
    		query.append("WHERE LOG_TKTM BETWEEN '"+ gijunStartDT +"' AND '" + gijunEndDT+"' ");
    		query.append(";");
			
    		stmt.execute(query.toString());
			
    	} catch(IOException e) {
    		e.printStackTrace();
    		Assert.fail();
    	} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		} finally { 
			if (rs != null) {
				try {rs.close();} catch (SQLException e) {e.printStackTrace();} 
				}
			if (stmt != null) {
				try { stmt.close(); } catch (SQLException e) {e.printStackTrace(); } 
				}
			if (conn != null) {
				try { conn.close(); } catch (SQLException e) {e.printStackTrace();} 
				}
		}	
    }
}