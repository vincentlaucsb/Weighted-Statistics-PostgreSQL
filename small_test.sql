CREATE TABLE small_weighted_test (value real, weight real);

INSERT INTO small_weighted_test VALUES (1, 5);
INSERT INTO small_weighted_test VALUES (2, 2.5);
INSERT INTO small_weighted_test VALUES (4, 1.25);

SELECT weighted_sum(value, weight) FROM small_weighted_test;

DROP TABLE IF EXISTS small_weighted_median_test;

CREATE TABLE small_weighted_median_test (value real, weight real);

INSERT INTO small_weighted_median_test VALUES (1, 100);
INSERT INTO small_weighted_median_test VALUES (2, 100);
INSERT INTO small_weighted_median_test VALUES (4, 25);

SELECT weighted_median(value, weight) FROM small_weighted_median_test;