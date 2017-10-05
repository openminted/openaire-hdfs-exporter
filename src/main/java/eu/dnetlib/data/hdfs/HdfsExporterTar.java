package eu.dnetlib.data.hdfs;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarHeader;
import org.kamranzafar.jtar.TarOutputStream;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.util.*;

public class HdfsExporterTar {
    public static void main(String[] args) throws ParseException, IOException {

        Options opt = new Options();
        opt.addOption("f", true, "Hadoop configuration file (mandatory)");
        opt.addOption("v", false, "verbose (optional)");
        opt.addOption("s", true, "starting point (optional)");
        opt.addOption("t", true, "number of total files to fetch (optional)");
        opt.addOption("z", true, "number of total files to zip (optional)");

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


        int start = 0;
        int total = 0;
        int zip = 0;

        if (cl.hasOption("s")) {
            start = Integer.parseInt(cl.getOptionValue("s"));
        }

        if (cl.hasOption("t")) {
            total = Integer.parseInt(cl.getOptionValue("t"));
        }

        if (cl.hasOption("z")) {
            zip = Integer.parseInt(cl.getOptionValue("z"));
        }

        Path path = new Path(p.getProperty("hdfs.exporter.seq.file"));
        String outDir = p.getProperty("hdfs.exporter.out.dir");

        for (Map.Entry<Object, Object> e : p.entrySet()) {
            conf.set(e.getKey().toString(), e.getValue().toString());
        }

        new HdfsExporterTar().export(conf, path, outDir, cl.hasOption("v"), start, total, zip);
    }

    private void export(Configuration conf, Path path, String outDir, boolean verbose, int start, int total, int zip) throws IOException {
        int pcount = 0;
        int rcount = 0;
        int dcount = 0;
        int ocount = 0;

        int spoint = 0; // starting point
        int fcount = 0; //files' count
        int doccount = 0; //files' count

        String url = "http://adonis.athenarc.gr:8082/bridge/insert";
        long startTime = System.currentTimeMillis();

        // Uncomment following lines to get old functionality (saving in files and stuff)
        File currentdir = new File(outDir + "/results");//
        FileUtils.forceMkdir(currentdir);

        Iterator<Pair<Text, Text>> pairIterator = SequenceFileUtils.read(path, conf).iterator();

        while (pairIterator.hasNext()) {

            while (start > 0 && spoint < start) {
                spoint++;
                pairIterator.next();
            }

            long startingTime = System.currentTimeMillis();

            final Pair<Text, Text> p = pairIterator.next();

            try {
                String subfile = p.getFirst().toString().split("\\|")[1];
//                String subfile = p.getFirst().toString().split("::")[1].substring(0, 3);
                //String subdir = p.getFirst().toString().split("::")[1].substring(0, 3);

                String type = "";
                if (p.getSecond().toString().contains("oaf:datasource")) {
                    type = "datasources";
//					dcount++;
                    continue;
                } else if (p.getSecond().toString().contains("oaf:project")) {
                    type = "projects";
//					pcount++;
                    continue;
                } else if (p.getSecond().toString().contains("oaf:organization")) {
                    type = "organizations";
//					ocount++;
                    continue;
                } else if (p.getSecond().toString().contains("oaf:person")) {
                    type = "persons";
                    continue;
                } else if (p.getSecond().toString().contains("oaf:result")) {
                    type = "results";
                    rcount++;
                } else {
                    throw new IllegalArgumentException("invalid entity type");
                }

                fcount = dcount + pcount + ocount + rcount;
                String entity;

                if (p.getSecond().toString().startsWith("<?xml ")) {
                    entity = p.getSecond().toString().substring(p.getSecond().toString().indexOf("?>") + 2);
                } else
                    entity = p.getSecond().toString();

                try {
                    if ((entity.contains("bestlicense")
                            &&
                            (entity.contains("classname=\"Open Access\"")
                                    || entity.contains("classname=\"Embargo\"")
                                    || entity.contains("classname=\"12 Months Embargo\"")
                                    || entity.contains("classname=\"6 Months Embargo\"")
                                    || entity.contains("classname=\"Restricted\"")

                            ))
                            &&
                            (entity.contains("resulttype") && entity.contains("classname=\"publication\""))
                            &&
                            (entity.contains("<deletedbyinference>false</deletedbyinference>"))
                            ) {
//                        int starting = entity.indexOf("<dri:objIdentifier>") + "<dri:objIdentifier>".length();
//                        int ending = entity.indexOf("</dri:objIdentifier>");
//                        String subfile = entity.substring(starting, ending);

                        StringReader reader = new StringReader(entity);
                        if (verbose) System.out.println("w: " + subfile);
                        String filename = currentdir + "/" + subfile + ".xml";

                        File xmlFile = new File(filename);
                        FileWriter writer = new FileWriter(filename, true);
                        IOUtils.copy(reader, writer);
                        reader.close();
                        writer.close();
                        doccount++;
                    }
                    if (total > 0) {
                        if (fcount % 100 == 0)
                            System.out.println("Progress: " + doccount + " files sent out of " + fcount + " (" + (double) ((fcount * 100) / total) + "%)");
                    } else {
                        if (fcount % 100 == 0)
                            System.out.println("Progress: " + doccount + " files sent out of " + fcount);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (zip > 0 && currentdir.listFiles().length > 0 && currentdir.listFiles().length % zip == 0) {
                    SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
                    requestFactory.setBufferRequestBody(false);
                    RestTemplate restTemplate = new RestTemplate(requestFactory);

                    long gatheringTime = System.currentTimeMillis();

                    System.out.println("Gathered " + currentdir.listFiles().length + " results in " + (gatheringTime - startingTime) + " milis... Time to tar them");

                    File tarFile = new File(outDir + "/results.tar");
                    FileOutputStream dest = new FileOutputStream(tarFile);
                    TarOutputStream out = new TarOutputStream(dest);
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.MULTIPART_FORM_DATA);
                    MultiValueMap<String, Object> map = new LinkedMultiValueMap<>();

                    map.add("file", new FileSystemResource(tarFile));
                    HttpEntity<MultiValueMap<String, Object>> httpEntity = new HttpEntity<>(map, headers);

                    for (File file : currentdir.listFiles()) {
//                        System.out.println(file.getName());
                        out.putNextEntry(new TarEntry(file, file.getName()));
                        FileReader reader1 = new FileReader(file);
                        IOUtils.copy(reader1, out);
                        out.flush();
                        file.delete();
                    }
                    out.close();

                    System.out.println("file size?: " + tarFile.length());
                    restTemplate.postForObject(url, httpEntity, String.class);//.postForObject(url, tarFile, File.class);
                    long finishingTime = System.currentTimeMillis();
                    System.out.println("Finished zipping and sending tar file to Rest endpoint at " + (finishingTime - gatheringTime) + " milis");
                    tarFile.delete();
                }

                if (total > 0 && fcount >= total) {
                    break;
                }
            } catch (Exception e) {
                System.err.println("e: " + p.getFirst() + " " + e.getMessage());
            }
        }

        if (currentdir.listFiles().length > 0) {
            System.out.println("Finishing with any xml left");

            SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
            requestFactory.setBufferRequestBody(false);
            RestTemplate restTemplate = new RestTemplate(requestFactory);

            File tarFile = new File(outDir + "/results.tar");
            FileOutputStream dest = new FileOutputStream(tarFile);
            TarOutputStream out = new TarOutputStream(dest);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);
            MultiValueMap<String, Object> map = new LinkedMultiValueMap<>();

            map.add("file", new FileSystemResource(tarFile));
            HttpEntity<MultiValueMap<String, Object>> httpEntity = new HttpEntity<>(map, headers);

            for (File file : currentdir.listFiles()) {
                System.out.println(file.getName());
                out.putNextEntry(new TarEntry(file, file.getName()));
                FileReader reader1 = new FileReader(file);
                IOUtils.copy(reader1, out);
                out.flush();
                file.deleteOnExit();
            }
            out.close();

            System.out.println("Sending tar file to Rest endpoint...");
            restTemplate.postForObject(url, httpEntity, String.class);//.postForObject(url, tarFile, File.class);
            tarFile.deleteOnExit();
        }

        System.out.println("Downloaded " + doccount + " results, " + pcount + " projects, " + ocount + " organizations, " + dcount + " datasources");
        System.out.println("Started from " + spoint + " file and downloaded " + doccount + " files in total");

        long end = System.currentTimeMillis();

        System.out.println("Total time for " + total + " files is " + (end - startTime) + " milis");
    }

    private List<File> listFilesForFolder(final File folder) {
        List<File> files = new ArrayList<File>();
        if (folder != null && folder.listFiles() != null)
            for (final File fileEntry : folder.listFiles()) {
                if (fileEntry != null) {
                    if (fileEntry.isDirectory()) {
                        files.addAll(listFilesForFolder(fileEntry));
                    } else {
                        files.add(fileEntry);
                    }
                }
            }
        return files;
    }

    private void deleteFilesForFolder(final File folder) {
        if (folder != null && folder.listFiles() != null)
            for (final File fileEntry : folder.listFiles()) {
                if (fileEntry != null) {
                    if (fileEntry.isDirectory()) {
                        deleteFilesForFolder(fileEntry);
                        fileEntry.delete();
                    } else {
                        fileEntry.delete();
                    }
                }
            }
    }
}
