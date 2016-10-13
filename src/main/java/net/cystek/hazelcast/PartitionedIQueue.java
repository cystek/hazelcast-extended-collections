package net.cystek.hazelcast;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * @author Jonathan Tanner
 */
public class PartitionedIQueue<E> extends ICollectionCommon<E> implements IQueue<E> {
	protected PartitionedISet<IEntry<E>> backingSet;
	protected IMap<String,IEntry<E>> metaMap;

	public PartitionedIQueue(HazelcastInstance hazelcastInstance, String name) {
		backingSet = new PartitionedISet<IEntry<E>>(hazelcastInstance, name);
		metaMap = hazelcastInstance.getMap(getName() + "-metadata-" + name);
	}

	@Override
	public LocalQueueStats getLocalQueueStats() {
		return null;
	}

	@Override
	public E poll() {
		try {
			return poll(600, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			return null;
		}
	}


	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		if(metaMap.tryLock("HEAD", timeout, unit)) {
			try {
				IEntry<E> head = metaMap.get("HEAD");
				if(head == null)
					return null;
				if(backingSet.backingMap.tryLock(head, timeout, unit)) {
					try {
						if(head.next == null) {
							metaMap.remove("HEAD");
							metaMap.remove("TAIL");
						} else {
							metaMap.put("HEAD", head.next, timeout, unit);
						}
						return head.val;
					} finally {
						backingSet.backingMap.unlock(head);
					}
				}
			} finally {
				metaMap.unlock("HEAD");
			}
		}
		return null;
	}

	@Override
	public E take() throws InterruptedException {
		E val;
		while((val = poll()) == null);
		return val;
	}

	@Override
	public boolean add(E e) {
		return offer(e);
	}

	@Override
	public boolean contains(Object o) {
		return false;
	}

	@Override
	public int drainTo(Collection<? super E> c) {
		return 0;
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		return 0;
	}

	@Override
	public boolean offer(E e) {
		try {
			return offer(e, 0, TimeUnit.SECONDS);
		} catch (InterruptedException e1) {
			return false;
		}
	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		if(metaMap.tryLock("TAIL", timeout, unit)) {
			try {
				IEntry<E> tail = metaMap.get("TAIL");
				if(tail == null) {
					tail = new IEntry<E>(e);
				}
				if(backingSet.backingMap.tryLock(tail, timeout, unit)) {
					try {
						return backingSet.add(tail, timeout, unit);
					} finally {
						backingSet.backingMap.unlock(tail);
					}
				}
			} finally {
				metaMap.unlock("TAIL");
			}
		}
		return false;
	}

	@Override
	public void put(E e) throws InterruptedException {
		while(!offer(e));
	}

	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE; //todo
	}

	@Override
	public boolean remove(Object o) {
		Iterator<E> it = iterator();
		while(it.hasNext()) {
			if(o.equals(it.next())) {
				it.remove();
				return true;
			}
		}
		return false;
	}

	@Override
	public E element() {
		metaMap.lock("HEAD");
		IEntry<E> head = metaMap.get("HEAD");
		metaMap.unlock("HEAD");
		if(head == null)
			return null;
		return head.val;
	}

	@Override
	public E peek() {
		E val = element();
		if(val == null)
			throw new NoSuchElementException("Empty queue.");
		return val;
	}

	@Override
	public E remove() {
		E val = poll();
		if(val == null)
			throw new NoSuchElementException("Empty queue.");
		return val;
	}

	@Override
	public String addItemListener(ItemListener<E> listener, boolean includeValue) {
		return null;
	}

	@Override
	public boolean removeItemListener(String registrationId) {
		return false;
	}

	@Override
	public int size() {
		return backingSet.size();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public Iterator<E> iterator() {
		return new PartitionedIQueueIterator();
	}

	@Override
	public void clear() {

	}

	@Override
	protected DistributedObject getBackingDistributedObject() {
		return backingSet;
	}

	protected class PartitionedIQueueIterator implements Iterator<E> {
		protected IEntry<E> entry, prev;

		public PartitionedIQueueIterator() {
			metaMap.lock("HEAD");
			entry = metaMap.get("HEAD");
			if(entry == null)
				metaMap.unlock("HEAD");
			else if(entry.next == null)
				metaMap.lock("TAIL");
		}

		@Override
		public boolean hasNext() {
			return entry != null;
		}

		@Override
		public E next() {
			E val = entry.val;
			if(metaMap.get("HEAD").equals(entry)) {
				entry = entry.next;
			}
			else {
				if(metaMap.get("HEAD").equals(prev)) {
					metaMap.unlock("HEAD");
				}
				else {
					backingSet.backingMap.unlock(prev);
				}
				prev = entry;
				entry = entry.next;
				if(entry == null)
					metaMap.lock("TAIL");
			}
			return val;
		}

		@Override
		public void remove() {
			if(prev == null) {
				IEntry<E> head = metaMap.get("HEAD");
				if(head != null) { //hasn't already been removed
					if(head.next == null) { //removing only entry
						metaMap.remove("HEAD");
						metaMap.remove("TAIL"); //todo locks
					}
					else
						metaMap.put("HEAD", head.next);
					backingSet.remove(head);
				}
			}
			else if(prev.next != entry) { //hasn't already been removed
				backingSet.remove(prev.next);
				prev.next = entry;
			}
		}
	}
}
