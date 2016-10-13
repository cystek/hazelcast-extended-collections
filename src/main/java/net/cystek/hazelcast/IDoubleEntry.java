package net.cystek.hazelcast;

/**
 * @author Jonathan Tanner
 */
public class IDoubleEntry<E> extends IEntry<E> {
	public IEntry<E> prev;

	public IDoubleEntry(E value) {
		super(value);
	}
}
