package ru.mephi.skok;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class BashTest {
	
	/*
	 * Creates new Generator and asserts if data.csv was created
	 */

    InputDataGenerator testGenerator = new InputDataGenerator();

    @Test
    public void generateBashInput() throws Exception
    {

        testGenerator.generateBashInput();
        assertEquals(true, new File("/home/cloudera/workspace/spark/src/main/resources/data.csv").exists());

    }

}
