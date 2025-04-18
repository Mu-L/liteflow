package com.yomahub.liteflow.util;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

public class BoundedPriorityBlockingQueue<E> extends PriorityBlockingQueue<E> {

	private static final long serialVersionUID = -1;

	// 容量
	private final int capacity;

	private final Comparator<? super E> comparator;

	public BoundedPriorityBlockingQueue(int capacity) {
		this(capacity, null);
	}

	/**
	 * 构造
	 * @param capacity 容量
	 * @param comparator 比较器
	 */
	public BoundedPriorityBlockingQueue(int capacity, final Comparator<? super E> comparator) {
		super(capacity, (o1, o2) -> {
			int cResult;
			if (comparator != null) {
				cResult = comparator.compare(o1, o2);
			}
			else {
				@SuppressWarnings("unchecked")
				Comparable<E> o1c = (Comparable<E>) o1;
				cResult = o1c.compareTo(o2);
			}

			return -cResult;
		});
		this.capacity = capacity;
		this.comparator = comparator;
	}

	/**
	 * 加入元素，当队列满时，淘汰末尾元素
	 * @param e 元素
	 * @return 加入成功与否
	 */
	@Override
	public boolean offer(E e) {
		if (size() >= capacity) {
			E head = peek();
			if (this.comparator().compare(e, head) <= 0) {
				return false;
			}
			// 当队列满时，就要淘汰顶端队列
			poll();
		}
		return super.offer(e);
	}

	/**
	 * 添加多个元素<br>
	 * 参数为集合的情况请使用{@link PriorityQueue#addAll}
	 * @param c 元素数组
	 * @return 是否发生改变
	 */
	public boolean addAll(E[] c) {
		return this.addAll(Arrays.asList(c));
	}

	/**
	 * @return 返回排序后的列表
	 */
	public ArrayList<E> toList() {
		final ArrayList<E> list = new ArrayList<>(this);
		list.sort(comparator);
		return list;
	}

	@Override
	public Iterator<E> iterator() {
		return toList().iterator();
	}

}
