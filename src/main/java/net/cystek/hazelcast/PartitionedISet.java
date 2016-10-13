package net.cystek.hazelcast;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A partitioned version of a Hazelcast ISet.
 * @author Jonathan Tanner
 */
public class PartitionedISet<E> extends ICollectionCommon<E> implements ISet<E> {
	protected IMap<E,Boolean> backingMap;

	public PartitionedISet(HazelcastInstance hazelcastInstance, String name) {
		backingMap = hazelcastInstance.getMap(getClass().getName() + "-" + name);
	}

	@Override
	public String addItemListener(final ItemListener<E> listener, boolean includeValue) {
		String add = backingMap.addEntryListener(new EntryAddedListener<E,Boolean>() {
			@Override
			public void entryAdded(EntryEvent<E,Boolean> event) {
				listener.itemAdded(new ItemEvent<E>(event.getName(), ItemEventType.ADDED, event.getKey(), event.getMember()));
			}
		}, includeValue);
		String remove = backingMap.addEntryListener(new EntryRemovedListener<E,Boolean>() {
			@Override
			public void entryRemoved(EntryEvent<E,Boolean> event) {
				listener.itemRemoved(new ItemEvent<E>(event.getName(), ItemEventType.REMOVED, event.getKey(), event.getMember()));
			}
		}, includeValue);
		return add.length() + ":" + add + ";" + remove.length() + ":" + remove;
	}

	@Override
	public boolean removeItemListener(String registrationId) {
		int len, idx = 0, idx2;
		idx2 = registrationId.indexOf(':');
		if(idx2 < 0)
			return false;
		len = Integer.parseInt(registrationId.substring(idx, idx2));
		idx = idx2 + 1;
		String add = registrationId.substring(idx, idx + len);
		idx += len;
		if(registrationId.charAt(idx) != ';')
			return false;
		idx++;
		idx2 = registrationId.indexOf(':', idx);
		if(idx2 < 0)
			return false;
		len = Integer.parseInt(registrationId.substring(idx, idx2));
		idx = idx2 + 1;
		String remove = registrationId.substring(idx, idx + len);
		return backingMap.removeEntryListener(add) && backingMap.removeEntryListener(remove);
	}

	@Override
	public int size() {
		return backingMap.size();
	}

	@Override
	public boolean isEmpty() {
		return backingMap.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return backingMap.containsKey(o);
	}

	@Override
	public Iterator<E> iterator() {
		return new DistributedISetIterator();
	}

	@Override
	public boolean add(E e) {
		return backingMap.put(e, true);
	}

	public boolean add(E e, long timeout, TimeUnit unit) {
		return backingMap.put(e, true, timeout, unit);
	}

	@Override
	public boolean remove(Object o) {
		return backingMap.remove(o);
	}

	@Override
	public void clear() {
		backingMap.clear();
	}

	@Override
	protected DistributedObject getBackingDistributedObject() {
		return backingMap;
	}

	public class DistributedISetIterator implements Iterator<E> {
		protected Iterator<Map.Entry<E,Boolean>> iterator = backingMap.entrySet().iterator();

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public E next() {
			return iterator.next().getKey();
		}

		@Override
		public void remove() {
			iterator.remove();
		}
	}
}
