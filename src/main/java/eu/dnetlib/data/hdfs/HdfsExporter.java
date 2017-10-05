package eu.dnetlib.data.hdfs;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class HdfsExporter {
    public static void main(String[] args) throws ParseException, IOException {

		Options opt = new Options();
		opt.addOption("f", true, "Hadoop configuration file (mandatory)");
		opt.addOption("v", false, "verbose (optional)");

		BasicParser parser = new BasicParser();
		CommandLine cl = parser.parse(opt, args);
		if (!cl.hasOption("f")) {
			System.out.println();
			HelpFormatter f = new HelpFormatter();
			f.printHelp("java -jar [JAR_FILE]", opt);
			System.out.println();
			return;
		}

		Configuration conf = new Configuration();
		Properties p = new Properties();
		p.load(new FileReader(cl.getOptionValue("f")));

		Path path = new Path(p.getProperty("hdfs.exporter.seq.file"));
		String outDir = p.getProperty("hdfs.exporter.out.dir");

		for(Map.Entry<Object, Object> e : p.entrySet()) {
			conf.set(e.getKey().toString(), e.getValue().toString());
		}

		new HdfsExporter().export(conf, path, outDir, cl.hasOption("v"));
    }

	private void export(Configuration conf, Path path, String outDir, boolean verbose) throws IOException {
		int pcount = 0;
		int rcount = 0;
		int dcount = 0;
		int ocount = 0;

		for(final Pair<Text, Text> p : SequenceFileUtils.read(path, conf)) {
			try {
				String subfile = p.getFirst().toString().split("::")[1].substring(0, 3);
				//String subdir = p.getFirst().toString().split("::")[1].substring(0, 3);

				String type = "";
				if (p.getSecond().toString().contains("oaf:datasource")) {
					type = "datasources"; dcount++;
				} else if (p.getSecond().toString().contains("oaf:project")) {
					type = "projects"; pcount++;
				} else if (p.getSecond().toString().contains("oaf:organization")) {
					type = "organizations"; ocount++;
				} else if (p.getSecond().toString().contains("oaf:person")) {
					type = "persons";continue;
				} else if (p.getSecond().toString().contains("oaf:result")) {
					type = "results";rcount++;
				} else {
					throw new IllegalArgumentException("invalid entity type");
				}

				File currentdir = new File(outDir + "/" + type );//
				FileUtils.forceMkdir(currentdir);
				String entity;

				if (p.getSecond().toString().startsWith("<?xml ")) {
    					entity = p.getSecond().toString().substring(p.getSecond().toString().indexOf("?>") + 2);
				}
				else
					entity = p.getSecond().toString();

				StringReader reader = new StringReader(entity);
				String filename = currentdir + "/" + subfile + ".xml";
				//String filename = currentdir + "/" + p.getFirst().toString().split("::")[1] + ".xml";
				FileWriter writer;
				
				if (!new File(currentdir + "/" + subfile + ".xml").exists()) {
					writer = new FileWriter(filename, false);

					writer.append("<records>");
					//IOUtils.closeQuietly(writer);
				}
				else{
					writer = new FileWriter(filename, true);
				}
				if (verbose) System.out.println("w: " + filename);

				IOUtils.copy(reader, writer);
				//IOUtils.closeQuietly(reader);
				//IOUtils.closeQuietly(writer);
				reader.close();
				writer.close();
			} catch(IOException e) {
				System.err.println("e: " + p.getFirst() + " " + e.getMessage());
			}
			/*finally{
				IOUtils.closeQuietly(reader);
				IOUtils.closeQuietly(writer);
			}*/
		}
		System.out.println("Downloaded "+rcount+" results, "+pcount+" projects, "+ocount+" organizations, "+dcount+" datasources");


	}
}
