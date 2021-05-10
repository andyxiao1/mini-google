package edu.upenn.cis.cis455;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class TestLemmas {
	
	@Test
	public void testLemmaSize() {
		StanfordLemmatizer s = new StanfordLemmatizer();
		
		String output = s.getLemma("Hello's");
		assertEquals(output, "hello");
//		s.getLemmasList("Hello's");
		
	}

}
