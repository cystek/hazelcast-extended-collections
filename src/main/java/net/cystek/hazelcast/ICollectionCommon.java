package net.cystek.hazelcast;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ICollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Jonathan Tanner
 */
public abstract class ICollectionCommon<E> implements ICollection<E> {
	protected int substringIndex = getClass().getName().length() + 1;

	@Override
	public boolean addAll(Collection<? extends E> c) {
		Set<? extends E> set = new HashSet<E>(c);
		for(Iterator<? extends E> it = set.iterator(); it.hasNext();)
			if(add(it.next()))
				it.remove();
		return set.size() < c.size();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		Set lookupSet = new HashSet();
		lookupSet.addAll(c);
		Iterator<E> it = iterator();
		while(it.hasNext() && !lookupSet.isEmpty()) {
			E val = it.next();
			if(lookupSet.contains(val))
				lookupSet.remove(val);
		}
		return lookupSet.isEmpty();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		Set set = new HashSet(c);
		boolean changed = false;
		for(Iterator<E> it = iterator(); it.hasNext();) {
			if(!set.contains(it.next())) {
				it.remove();
				changed = true;
			}
		}
		return changed;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		Set set = new HashSet(c);
		for(Iterator<E> it = iterator(); it.hasNext() && !set.isEmpty();) {
			E entry = it.next();
			if(set.contains(entry)) {
				it.remove();
				set.remove(entry);
			}
		}
		return c.size() > set.size();
	}

	@Override
	public Object[] toArray() {
		ArrayList<E> list = new ArrayList<E>();
		for(Iterator<E> it = iterator(); it.hasNext();)
			list.add(it.next());
		return list.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		Iterator<E> it = iterator();
		for(int i = 0; i < a.length && it.hasNext(); i++)
			a[i] = (T)it.next();
		if(it.hasNext()) {
			ArrayList<T> overflow = new ArrayList<T>();
			while(it.hasNext())
				overflow.add((T)it.next());
			a = Arrays.copyOf(a, a.length + overflow.size());
			for(int i = 0; i < overflow.size(); i++)
				a[i+a.length] = overflow.get(i);
		}
		return a;
	}

	@Override
	public String getPartitionKey() {
		return getBackingDistributedObject().getPartitionKey();
	}

	@Override
	public String getName() {
		return getBackingDistributedObject().getName().substring(substringIndex);
	}

	@Override
	public String getServiceName() {
		return getBackingDistributedObject().getServiceName();
	}

	@Override
	public void destroy() {
		getBackingDistributedObject().destroy();
	}

	protected abstract DistributedObject getBackingDistributedObject();
}
