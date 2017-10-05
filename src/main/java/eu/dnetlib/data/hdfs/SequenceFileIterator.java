package eu.dnetlib.data.hdfs;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * <p>{@link java.util.Iterator} over a {@link SequenceFile}'s keys and values, as a {@link Pair}
 * containing key and value.</p>
 */
public final class SequenceFileIterator<K extends Writable,V extends Writable> extends AbstractIterator<Pair<K,V>> implements Closeable {

	private final SequenceFile.Reader reader;
	private final Configuration conf;
	private final Class<K> keyClass;
	private final Class<V> valueClass;
	private final boolean noValue;
	private K key;
	private V value;
	private final boolean reuseKeyValueInstances;

	/**
	 * @throws IOException if path can't be read, or its key or value class can't be instantiated
	 */
	public SequenceFileIterator(Path path, boolean reuseKeyValueInstances, Configuration conf) throws IOException {
		key = null;
		value = null;
		FileSystem fs = path.getFileSystem(conf);
		path = path.makeQualified(fs);
		reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
		this.conf = conf;
		keyClass = (Class<K>) reader.getKeyClass();
		valueClass = (Class<V>) reader.getValueClass();
		noValue = NullWritable.class.equals(valueClass);
		this.reuseKeyValueInstances = reuseKeyValueInstances;
	}

	public Class<K> getKeyClass() {
		return keyClass;
	}

	public Class<V> getValueClass() {
		return valueClass;
	}

	@Override
	public void close() {
		key = null;
		value = null;
		Closeables.closeQuietly(reader);
		endOfData();
	}

	@Override
	protected Pair<K,V> computeNext() {
		if (!reuseKeyValueInstances || value == null) {
			key = ReflectionUtils.newInstance(keyClass, conf);
			if (!noValue) {
				value = ReflectionUtils.newInstance(valueClass, conf);
			}
		}
		try {
			boolean available;
			if (noValue) {
				available = reader.next(key);
			} else {
				available = reader.next(key, value);
			}
			if (!available) {
				close();
				return null;
			}
			return new Pair<K,V>(key, value);
		} catch (IOException ioe) {
			close();
			throw new IllegalStateException(ioe);
		}
	}

}