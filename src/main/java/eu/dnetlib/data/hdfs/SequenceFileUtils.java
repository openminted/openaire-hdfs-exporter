package eu.dnetlib.data.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class SequenceFileUtils {
	
	public static Iterable<Pair<Text, Text>> read(Path path, final Configuration conf) throws IOException {
		return Iterables.concat(
					Iterables.transform(
							new SequenceFileUtils().doRead(path, conf), 
							new Function<Path, SequenceFileIterable<Text, Text>>() {
								@Override
								public SequenceFileIterable<Text, Text> apply(Path path) {
									return new SequenceFileIterable<Text, Text>(path, conf);
								}
							}));
	}
	
    private PathFilter f = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return path.getName().contains("part-r");
		}
	};
	
	private Iterable<Path> doRead(Path path, final Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		return Iterables.transform(Lists.newArrayList(fs.listStatus(path, f)), new Function<FileStatus, Path>() {
	    	@Override
	    	public Path apply(FileStatus fs) {
				Path path = fs.getPath();
				System.out.println("downloading xml files from path: " + path.toString());
				return path;		
	    	}
	    });
	}

}
