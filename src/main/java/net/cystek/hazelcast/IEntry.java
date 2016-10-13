package net.cystek.hazelcast;

/**
 * @author Jonathan Tanner
 */
class IEntry<E> {
	public E val;
	public IEntry<E> next;

	public IEntry(E value) {
		val = value;
	}
}
