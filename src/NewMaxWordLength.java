import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A Hadoop Map / Reduce job written for the 0.21 API.
 * 
 * The mapper function reads each line of a file, then outputs
 * the length of the longest word as a key and the word itself
 * as the value.
 * 
 * The reducer function combines and alphabetically arranges all
 * words of the same length.
 * 
 * The output file created by this job will print the length of
 * the longest word(s) and sample words of that length on the
 * last line of the file.
 * 
 * Usage: NewMaxWordLength <input path> <output path>
 * 
 * @author Kourtni Marshall
 *
 */

public class NewMaxWordLength {
	public static class NewMaxWordlengthMapper
		extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		private Text longestWord = new Text();
		List<String> wordList = new ArrayList<String>();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			HashSet<Character> badChars = new HashSet<Character>();
			badChars.add('.');badChars.add(',');badChars.add('?');badChars.add('!');
			badChars.add('\'');badChars.add('\"');badChars.add(';');badChars.add(':');
			badChars.add('|');badChars.add('[');badChars.add(']');badChars.add('(');
			badChars.add(')');badChars.add('{');badChars.add('}');badChars.add('&');
			badChars.add('@');badChars.add('#');badChars.add('$');badChars.add('%');
			badChars.add('=');badChars.add('_');badChars.add('~');badChars.add('-');
			badChars.add('<');badChars.add('>');badChars.add('*');badChars.add('^');
			String[] line = value.toString().split("\\s+");
			wordList.clear();
			for(int i = 0; i < line.length; ++i){
				String[] temp = line[i].split("--");
				for(int j = 0; j < temp.length; ++j){
					wordList.add(temp[j]);
				}
			}
			longestWord.set("");
			for(String word : wordList){
				while(word.length() > 0 && badChars.contains(word.charAt(word.length()-1))){
					word = word.substring(0, word.length()-1);
				}
				while(word.length() > 0 && badChars.contains(word.charAt(0))){
					word = word.substring(1, word.length());
				}
				if(word.trim().length() > longestWord.toString().length()
						&& !word.contains(":") //&& !word.contains("-") //uncomment to exclude dashed words
						&& !word.contains("/") && !word.contains("@")
						&& !word.contains(".") && !word.contains(","))
					longestWord.set(word.trim().toLowerCase());
			}
			// emits the length of the longest word in a line and the word itself
			if(longestWord.toString().length() > 0)
				context.write(new IntWritable(longestWord.toString().length()), longestWord);
		}
	
	}
	
	public static class NewMaxWordLengthReducer
		extends Reducer<IntWritable, Text, IntWritable, Text>{
		
		private Text wordsOfKeyLength = new Text();
		private TreeSet<String> words = new TreeSet<String>();
		private String tempString;
		
		public synchronized void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			words.clear();
			for(Text value : values){
				tempString = value.toString();
				if(tempString.charAt(0) == '['){
					tempString = tempString.substring(1, tempString.length()-1);
					String[] wArray = tempString.split(", ");
					for(int i = 0; i < wArray.length; ++i){
						words.add(wArray[i]);
					}
				}
				else
					words.add(tempString);
			}
			wordsOfKeyLength.set(words.toString());
			context.write(key, wordsOfKeyLength);
		}
	
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.err.println("Usage: NewMaxWordLength <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(NewMaxWordLength.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(NewMaxWordlengthMapper.class);
		job.setCombinerClass(NewMaxWordLengthReducer.class);
		job.setReducerClass(NewMaxWordLengthReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		if(job.waitForCompletion(true)){
			try{	//Code in try block is not supported in HDFS
				File path = new File(args[1]);
				String[] files = path.list();
				String file = args[1] + "/" + files[0].substring(1, files[0].length()-4);
				
				FileInputStream in = new FileInputStream(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String strLine = null, tmp;
				while ((tmp = br.readLine()) != null){
				   strLine = tmp;
				}
	
				String[] lastLine = strLine.split("\t");	
				in.close();
	
				System.out.println("\nThe Map Reduce program completed and created the file: " +
						file);
				System.out.println("The longest word(s) found in "+ args[0] +" equal(s) " + lastLine[0]
				                    + " characters long. The results are below.\n");
				System.out.println("Longest word(s) found in " + args[0] + " = "
						+ lastLine[1].substring(1, lastLine[1].length()-1) + "\n");
			} catch(Exception e){
				System.out.println("Result Report not supported by current filesystem.\n" +
						"Please check directory " + args[1] + " for output file.\n");
			}
			System.exit(0);
		}
		
		System.exit(1);
	}
}