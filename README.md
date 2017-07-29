# Weighted Statistics for PostgreSQL
This project provides functions for calculating statistics not provided in the standard PostgreSQL library. Currently, the following statistics can be calculated:
 * Weighted Sum
 * Weighted Average
 * Weighted Median
 * Median Absolute Deviation

These functions implement efficient algorithms in pure PL/pgSQL, and require only built-in extensions.
 
## Development Status: Alpha
The functions work but I haven't created tests for them yet. Use at your own peril!

## Functions
For all functions listed below, the first argument should be a column of values, while the second should be a column of weights.

### Unweighted Statistics
median_absolute_deviation(numeric)

### Weighted Statistics
weighted_sum(numeric, numeric)
weighted_avg(numeric, numeric)
weighted_median(numeric, numeric)