package mil.nga.giat.geowave.ingest.accumulo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

public class OSMConversionIterator extends WholeRowIterator {
	
	private Map<String, String> _options;
	
	
	@Override
	protected boolean filter(Text currentRow, List<Key> keys, List<Value> values) {
		String columnQualifiers = _options.get("cq");
		String[] cq = columnQualifiers.split(",");
		for (String s : cq) {
			for (Key k : keys){
				if (k.toString().contains(s))
					return true;
			}
		}
		return false;
	}
	
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, 	Map<String, String> options, IteratorEnvironment env) throws IOException {
		_options = options;
		super.init(source, options, env);
	}

}
