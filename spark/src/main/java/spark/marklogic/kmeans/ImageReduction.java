package spark.marklogic.kmeans;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ImageReduction {
	private static JavaSparkContext javaSparkContext;

	private static JavaSparkContext getSparkContext() {
		if (javaSparkContext == null) {
			SparkConf conf = new SparkConf().setAppName("Image Reduction with KMeans").setMaster("local[*]");
			javaSparkContext = new JavaSparkContext(conf);
		}
		return javaSparkContext;
	}

	public static void main(String args[]) throws IOException {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getRootLogger().setLevel(Level.ERROR);

		String fileName = args[0];
		Properties prop = new Properties();
		FileInputStream input = null;
		input = new FileInputStream(fileName);
		System.out.println("Loading properties... " + fileName);
		prop.load(input);

		BufferedImage image = null;
		File f = null;
		KMeansOperations operations = new KMeansOperations();

		// read image
		try {
			f = new File(prop.getProperty("imageInputPath"));
			image = ImageIO.read(f);
			System.out.println("Reading complete.");
		} catch (IOException e) {
			System.out.println("Error: " + e);
		}

		int[][] imageToTwoDimensionalMatrix = operations.transformImageToTwoDimensionalMatrix(image);

		Set<Integer> colors = new HashSet<Integer>();
		int w = image.getWidth();
		int h = image.getHeight();
		for (int y = 0; y < h; y++) {
			for (int x = 0; x < w; x++) {
				int pixel = image.getRGB(x, y);
				colors.add(pixel);
			}
		}
		System.out.println("There are " + colors.size() + " colors in the input image");

		int[][] transformedImageMatrix = operations.runKMeans(getSparkContext(), imageToTwoDimensionalMatrix,
				Integer.parseInt(prop.getProperty("colorToReduce")));

		InputStream inputStream = operations.reCreateOriginalImageFromMatrix(image, transformedImageMatrix,
				prop.getProperty("imageOutputPath"));

	}// main() ends here
}// class ends here