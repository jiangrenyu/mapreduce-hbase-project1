package com.bonc.hbaseMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonc.hbaseMR.util.OssFileNameTextOutputFormat;


public class Main extends Configured implements Tool{

	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	
	private Configuration conf;
	
	@Override
	public void setConf(Configuration conf) {
		if(this.conf == null){
			this.conf = conf;
		}
	}

	@Override
	public Configuration getConf() {
		if(this.conf == null){
			return HBaseConfiguration.create();
		}
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		String kvName = "";
		String outputPath = "";
		String jobName = "";
		String confPath = "";
		String hbaseTableName = "";
		String startTime = "null";
		String endTime = "null";
		String prov = "";
		String date = "";
		
		
		if(args.length > 0){
			for(int i = 0; i<= args.length-1;i++){
				switch(args[i]){
				case "-kvName":
					kvName = args[i+1];
					break;
				case "-outputPath":
					outputPath = args[i+1];
					break;
				case "-jobName":
					jobName = args[i+1];
					break;
				case "-startTime":
					startTime = args[i+1];
					break;
				case "-endTime" :
					endTime = args[i+1];
					break;
				case "-confPath":
					confPath = args[i+1];
					break;
				case "-hbaseTableName":
					hbaseTableName = args[i+1];
					break;
				case "-prov":
					prov = args[i+1];
					break;
				case "-date":
					date = args[i+1];
					break;
				}
			}
			
			if(hbaseTableName.equals("") || kvName.equals("") || outputPath.equals("") || confPath.equals("") || jobName.equals("") || prov.equals("") || date.equals("")){
				LOG.error("请输入完整参数");
				System.exit(-1);
			}
			
			Path path = new Path(confPath);
			conf.addResource(path);
		}else{
			LOG.error("请输入参数");
			System.exit(-1);
		}
		
		conf.set("conf.date", date);
		conf.set("conf.prov_id", prov);
		conf.set("conf.startTime", startTime);
		conf.set("conf.endTime", endTime);
		conf.set("conf.kvName", kvName);
		
		Path outPath = new Path(outputPath);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath,true);
		}
		
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(Main.class);
		
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(hbaseTableName, scan, BoncTableMapper.class, NullWritable.class, Text.class, job);
		
		job.setNumReduceTasks(0);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileOutputFormat.setOutputPath(job, outPath);
	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		LazyOutputFormat.setOutputFormatClass(job, OssFileNameTextOutputFormat.class);
		
		return  job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Main(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
