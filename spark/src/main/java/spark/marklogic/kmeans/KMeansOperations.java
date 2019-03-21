package spark.marklogic.kmeans;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import javax.imageio.ImageIO;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class KMeansOperations {

	public int[][] transformImageToTwoDimensionalMatrix(BufferedImage img) {
		int[][] imageRGB = new int[img.getWidth() * img.getHeight()][3];
		int w = img.getWidth();
		int h = img.getHeight();
		int index = 0;
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++) {
				Color color = new Color(img.getRGB(i, j), true);
				imageRGB[index][0] = color.getRed();
				imageRGB[index][1] = color.getGreen();
				imageRGB[index][2] = color.getBlue();
				index++;

			}
		}
		return imageRGB;
	}

	public int[][] runKMeans(JavaSparkContext sparkContext, int[][] imageToTwoDimensionalMatrix, int colorToReduce) {
		KMeans kMeans = new KMeans();
		kMeans.setSeed(1).setK(colorToReduce);
		java.util.List<Vector> collect = Arrays.stream(imageToTwoDimensionalMatrix).map(e -> {
			DoubleStream doubleStream = Arrays.stream(e).mapToDouble(i -> i);
			double[] doubles = doubleStream.toArray();
			Vector dense = Vectors.dense(doubles);
			return dense;
		}

		).collect(Collectors.toList());

		JavaRDD<Vector> parallelize = sparkContext.parallelize(collect);
		KMeansModel fit = kMeans.run(parallelize.rdd());
		Vector[] clusters = fit.clusterCenters();
		int[][] transformedImage = new int[imageToTwoDimensionalMatrix.length][3];
		int index = 0;
		for (int[] ints : imageToTwoDimensionalMatrix) {
			double[] doubles = Arrays.stream(ints).mapToDouble(e -> e).toArray();
			int predict = fit.predict(Vectors.dense(doubles));
			transformedImage[index][0] = (int) clusters[predict].apply(0);
			transformedImage[index][1] = (int) clusters[predict].apply(1);
			transformedImage[index][2] = (int) clusters[predict].apply(2);
			index++;
		}
		return transformedImage;
	}

	public InputStream reCreateOriginalImageFromMatrix(BufferedImage originalImage, int[][] imageRGB, String path)
			throws IOException {
		BufferedImage writeBackImage = new BufferedImage(originalImage.getWidth(), originalImage.getHeight(),
				BufferedImage.TYPE_INT_RGB);
		int index = 0;
		for (int i = 0; i < originalImage.getWidth(); i++) {
			for (int j = 0; j < originalImage.getHeight(); j++) {
				Color color = new Color(imageRGB[index][0], imageRGB[index][1], imageRGB[index][2]);
				writeBackImage.setRGB(i, j, color.getRGB());
				index++;
			}
		}
		File outputFile = new File(path);
		ImageIO.write(writeBackImage, "jpg", outputFile);

		Set<Integer> colors = new HashSet<Integer>();
		int w = writeBackImage.getWidth();
		int h = writeBackImage.getHeight();
		for (int y = 0; y < h; y++) {
			for (int x = 0; x < w; x++) {
				int pixel = writeBackImage.getRGB(x, y);
				colors.add(pixel);
			}
		}
		System.out.println("There are " + colors.size() + " colors in the output image.");

		return new FileInputStream(path);
	}
}