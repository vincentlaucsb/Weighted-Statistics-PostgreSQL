-- From https://stackoverflow.com/questions/2913368/sorting-array-elements
CREATE OR REPLACE FUNCTION array_sort (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL
AS $$
SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$$;

CREATE FUNCTION _weighted_add(current_total numeric, value numeric, weight numeric) RETURNS numeric AS $$
BEGIN
	RETURN current_total + (value * weight);
END;
$$ LANGUAGE plpgsql;

CREATE AGGREGATE weighted_sum(numeric, numeric) (
    SFUNC=_weighted_add,
    STYPE=numeric,
    INITCOND=0
);

CREATE EXTENSION intarray;
CREATE EXTENSION hstore;

-- Weighted Average
CREATE OR REPLACE FUNCTION _rolling_avg(accumulator double precision[], value numeric, weight numeric)
RETURNS double precision[] AS $$
DECLARE
    mean_x double precision := accumulator[1];
    n bigint := accumulator[2]::bigint;
    mean_y double precision := value;
    m bigint := weight;
BEGIN
    IF weight = 0 THEN -- Do nothing
        RETURN accumulator;
    ELSE
        RETURN ARRAY[mean_x - (m*mean_x - m*mean_y)/(n + m), n + m];
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION _final_weighted_avg(accumulator double precision[])
RETURNS double precision AS $$
BEGIN
    RETURN accumulator[1];
END;
$$ LANGUAGE plpgsql;

CREATE AGGREGATE weighted_avg(numeric, numeric) (
    SFUNC=_rolling_avg,
    STYPE=double precision[],
    FINALFUNC=_final_weighted_avg,
    INITCOND='{0, 0}' -- First value is mean, second is N
)

CREATE FUNCTION _update_count(counts hstore, value numeric, weight numeric) RETURNS hstore AS $$
DECLARE
    sum_ numeric;
    n numeric := counts -> 'n';
    value text := value::text;
BEGIN
    -- Increment number of observations seen so far
    n := n + weight;

    IF exist(counts, value) THEN
        -- Update count
        sum_ := (counts -> value)::numeric + weight;
    ELSE
        sum_ := weight;
    END IF;
    
    RETURN counts || hstore(ARRAY[value, sum_::text, 'n', n::text]);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION _final_median(counts hstore)
RETURNS numeric AS $$
DECLARE
    -- Requires extension intarray
    -- Sorts all the numbers we've encountered so far
    numbers numeric[] := array_sort(array_remove(akeys(counts), 'n')::numeric[]);
    n       numeric := counts -> 'n';
    cum_sum numeric := 0;
    x       numeric;
BEGIN
    -- Loop over numbers until cumulative sum is half or more of total observations
    -- Then return number where we stopped
    FOREACH x IN ARRAY numbers
    LOOP
        cum_sum := cum_sum + (counts -> x::text)::numeric;
        IF cum_sum >= n/2 THEN
            RETURN x;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Arg 1: Values
-- Arg 2: Weights
CREATE AGGREGATE weighted_median(numeric, numeric) (
    SFUNC=_update_count,
    STYPE=hstore,
    FINALFUNC=_final_median,
    INITCOND='n=>0'
)

/*
Much slower JSONB version. Kept just for future benchmarking purposes.

CREATE FUNCTION _update_count(counts jsonb, value numeric, weight numeric) RETURNS jsonb AS $$
DECLARE
    sum_ numeric;
BEGIN
    -- Increment number of observations seen so far
    counts := counts || jsonb_build_object('n', (counts ->> 'n')::numeric + weight);

    IF counts ? value::text THEN
        sum_ := (counts ->> value::text)::numeric + weight;
        RETURN counts || jsonb_build_object(value::text, sum_);
    ELSE
        RETURN counts || jsonb_build_object(value::text, weight);
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION _final_median(counts jsonb)
RETURNS numeric AS $$
DECLARE
    -- Requires extension intarray
    -- Sorts all the numbers we've encountered so far
    numbers numeric[] := sort(ARRAY(SELECT j::int FROM jsonb_object_keys(counts) j WHERE j != 'n'));
    n       numeric := (counts ->> 'n')::numeric;
    cum_sum numeric := 0;
    x       numeric;
BEGIN
    -- Loop over numbers until cumulative sum is half or more of total observations
    -- Then return number where we stopped
    FOREACH x IN ARRAY numbers
    LOOP
        cum_sum := cum_sum + (counts ->> x::text)::numeric;
        IF cum_sum >= n/2 THEN
            RETURN x;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Arg 1: Values
-- Arg 2: Weights
CREATE AGGREGATE weighted_median(numeric, numeric) (
    SFUNC=_update_count,
    STYPE=jsonb,
    FINALFUNC=_final_median,
    INITCOND='{"n":0}'
)

*/

-- Uses almost the same functions as weighted median but with weight = 1
CREATE FUNCTION _update_count_unweighted(counts jsonb, value numeric) RETURNS jsonb AS $$
BEGIN
    RETURN _update_count(counts, value, 1::numeric);
END;
$$ LANGUAGE plpgsql;

CREATE AGGREGATE median(numeric) (
    SFUNC=_update_count_unweighted,
    STYPE=jsonb,
    FINALFUNC=_final_median,
    INITCOND='{"n":0}'
)

--- Calculate median first then calculate deviations
CREATE TYPE _mad_temp AS (counts jsonb, values numeric[]);

CREATE FUNCTION _update_mad_temp(temp _mad_temp, value numeric)
RETURNS _mad_temp AS $$
BEGIN
    temp.counts := _update_count(temp.counts, value, 1::numeric);
    temp.values := array_append(temp.values, value);
    
    RETURN temp;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _final_median_absolute_deviation(temp _mad_temp)
RETURNS numeric AS $$
DECLARE
    x numeric;
    index numeric;
    median numeric := _final_median(temp.counts);
    absolute_deviations numeric[];
BEGIN
    FOREACH x IN ARRAY temp.values LOOP
        absolute_deviations := array_append(absolute_deviations, abs(x - median));
    END LOOP;
    
    absolute_deviations := array_sort(absolute_deviations);
    index := round(array_length(absolute_deviations, 1)/2);
    
    RETURN absolute_deviations[index];
END;
$$ LANGUAGE plpgsql;

CREATE AGGREGATE median_absolute_deviation(numeric) (
    SFUNC=_update_mad_temp,
    STYPE=_mad_temp,
    FINALFUNC=_final_median_absolute_deviation,
    INITCOND='("{""n"": 0}","{}")'
)