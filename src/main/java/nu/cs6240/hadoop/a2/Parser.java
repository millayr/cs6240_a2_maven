package nu.cs6240.hadoop.a2;

/**
 * This class parses a line of input to extract the category and sales fields.
 * 
 * @author jan vitek
 */
final class Parser {

	String product;
	Double sales;

	/**
	 * Parse one line of input and extract a product category (String) and a price
	 * (Double).
	 * 
	 * @author jan
	 * @param line
	 *            one line of input, the expected format is six tab separated columns.
	 * @return this
	 */
	final Parser parse(final String line) {
		final String[] data = line.trim().split("\t");
		if (data.length == 6) {
			product = data[3];
			try {
				sales = Double.parseDouble(data[4]);
			} catch (NumberFormatException n) {}
		} else {
			product = null;
			sales = null;
		}
		return this;
	}

	/**
	 * @author jan
	 * @return true if the last line read had a valid product and sales.
	 */
	final boolean valid() {
		return product != null && sales != null;
	}
}