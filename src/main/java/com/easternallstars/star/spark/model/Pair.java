package com.easternallstars.star.spark.model;

import java.util.Objects;

public class Pair<A, B> {
	public final A fst;
	public final B snd;

	public Pair(A paramA, B paramB) {
		this.fst = paramA;
		this.snd = paramB;
	}

	public String toString() {
		return "Pair[" + this.fst + "," + this.snd + "]";
	}

	@SuppressWarnings("rawtypes")
	public boolean equals(Object paramObject) {
		if (!(paramObject instanceof Pair)) {
			return false;
		}
		return (Objects.equals(this.fst, ((Pair) paramObject).fst))
				&& (Objects.equals(this.snd, ((Pair) paramObject).snd));
	}

	public int hashCode() {
		if (this.fst == null) {
			return this.snd == null ? 0 : this.snd.hashCode() + 1;
		}
		if (this.snd == null) {
			return this.fst.hashCode() + 2;
		}
		return this.fst.hashCode() * 17 + this.snd.hashCode();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <A, B> Pair<A, B> of(A paramA, B paramB) {
		return new Pair(paramA, paramB);
	}
}