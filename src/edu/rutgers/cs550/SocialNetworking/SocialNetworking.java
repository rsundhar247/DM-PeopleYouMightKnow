package edu.rutgers.cs550.SocialNetworking;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SocialNetworking extends Configured implements Tool {

	public static final String CLASSNAME = "SocialNetworking";
	

	public static void main(String[] args) {
		String METHODNAME = "SocialNetworking()";
		
		System.out.println(CLASSNAME + " : "+ METHODNAME + " " + "Hello you are in main() of SocialNetworking !!!");
		
		try {
			ToolRunner.run(new Configuration(), new SocialNetworking(), args);
		} catch (Exception e) {
			System.out.println(CLASSNAME + " : "+ METHODNAME + " " + "Exception occured in main() -> ToolRunner :: " + e);
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String METHODNAME = "run()";
		System.out.println(CLASSNAME + " : "+ METHODNAME + " : Entering!! ");
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "SocialNetworking");
		job.setJarByClass(SocialNetworking.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(ValObject.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		System.out.println(CLASSNAME + " : "+ METHODNAME + " : Exiting!! ");
		return 0;
	}
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, ValObject> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] currLine = value.toString().split("\t");
		  
			if(currLine.length == 2){
				StringTokenizer tokens = new StringTokenizer(currLine[1], ",");
				Long primaryUser = Long.parseLong(currLine[0]);
				ArrayList<Long> secondaryUsers = new ArrayList<Long>();
		    	  
				while(tokens.hasMoreTokens()){
					Long secUser = Long.parseLong(tokens.nextToken());
					secondaryUsers.add(secUser);
					context.write(new LongWritable(primaryUser), new ValObject(secUser, -1L));
				}
	    		  
				for(int i = 0; i<secondaryUsers.size(); i++){
					for(int j = i+1; j<secondaryUsers.size(); j++) {
						context.write(new LongWritable(secondaryUsers.get(i)), new ValObject(secondaryUsers.get(j), primaryUser));
						context.write(new LongWritable(secondaryUsers.get(j)), new ValObject(secondaryUsers.get(i), primaryUser));
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<LongWritable, ValObject, LongWritable, Text> {
	      
		public void reduce(LongWritable key, Iterable<ValObject> values, Context context) throws IOException, InterruptedException {
			
			final HashMap<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();
			for (ValObject val : values) {
				boolean existing = false;
				if(val.mutualFriend == -1)
					existing = true;
		        
				Long sUser = val.secondaryUser;
				Long mutualFriend = val.mutualFriend;
	
				if (mutualFriends.containsKey(sUser)) {
					if (existing) {
						mutualFriends.put(sUser, null);
					} else if (mutualFriends.get(sUser) != null) {
						mutualFriends.get(sUser).add(mutualFriend);
					}
				} else {
					if (!existing) {
						ArrayList<Long> mFriend =  new ArrayList<Long>();
						mFriend.add(mutualFriend);
						mutualFriends.put(sUser, mFriend);
					} else {
						mutualFriends.put(sUser, null);
					}
				}
			}
		   
			SortedMap<Long, List<Long>> sortedMutualFriends = new TreeMap<Long, List<Long>>(new Comparator<Long>() {
				@Override
				public int compare(Long var1, Long var2) {
					int value1 = mutualFriends.get(var1).size(), value2 = mutualFriends.get(var2).size();
	                if (value1 > value2) {
	                    return -1;
	                } else if (value1 == value2 && var1 < var2) {
	                    return -1;
	                } else {
	                    return 1;
	                }
				}
	        });
	
			for (java.util.Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()) {
				if (entry.getValue() != null) {
					sortedMutualFriends.put(entry.getKey(), entry.getValue());
				}
			}
	
			String output = "";
			for (java.util.Map.Entry<Long, List<Long>> entry1 : sortedMutualFriends.entrySet()) {
				if(output.equals(""))
					output = entry1.getKey().toString();
				else
					output += ","+entry1.getKey().toString();
			}
			context.write(key, new Text(output));
			
		}
	}

	public static class ValObject implements Writable {
	   
		public Long secondaryUser;
		public Long mutualFriend;
		
		public ValObject(Long sUser, Long mFriend){
			this.secondaryUser = sUser;
			this.mutualFriend = mFriend;
		}
		
		public ValObject() {
			this(-1L, -1L);
		}
		   
		@Override
		public void readFields(DataInput arg0) throws IOException {
			secondaryUser = arg0.readLong();
			mutualFriend = arg0.readLong();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			arg0.writeLong(secondaryUser);
			arg0.writeLong(mutualFriend);
		}
	}
}