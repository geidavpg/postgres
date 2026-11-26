DO $$
BEGIN
    -- Table t
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 't') THEN
        CREATE TABLE t(id bigserial primary key, order_date date, amount numeric(18, 2));
        INSERT INTO t(order_date, amount) SELECT DATE '2024-01-01' + (gs % 730), gs::numeric(18, 2) * gs % 998244353 * gs % 1000000007 * 133.31 FROM generate_series(1, 1000000) AS gs;
        -- Regenerate COPY source files (see below) whenever t is recreated
        EXECUTE 'COPY (SELECT amount FROM t) TO ''/tmp/numeric_bench.bin'' WITH (FORMAT binary)';
        EXECUTE 'COPY (SELECT amount FROM t) TO ''/tmp/numeric_bench.txt''';
    END IF;
 
    -- Table t_big
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 't_big') THEN
        CREATE TABLE t_big(id bigserial primary key, order_date date, amount numeric(38, 2));
        INSERT INTO t_big(order_date, amount) SELECT DATE '2024-01-01' + (gs % 730), gs::numeric(38, 2) * gs * gs * 133.31 FROM generate_series(1, 1000000) AS gs;
    END IF;
 
    -- Table t_small
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 't_small') THEN
        CREATE TABLE t_small(id bigserial primary key, order_date date, amount numeric(9, 2));
        INSERT INTO t_small(order_date, amount) SELECT DATE '2024-01-01' + (gs % 730), (gs % 1331) * (gs % 1331) * 1.31 FROM generate_series(1, 1000000) AS gs;
    END IF;

    -- Table t_special: only NaN / Infinity / -Infinity values
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 't_special') THEN
        CREATE TABLE t_special(id bigserial primary key, order_date date, amount numeric);
        INSERT INTO t_special(order_date, amount) SELECT DATE '2024-01-01' + (gs % 730), CASE gs % 3 WHEN 0 THEN 'NaN'::numeric WHEN 1 THEN 'Infinity'::numeric ELSE '-Infinity'::numeric END FROM generate_series(1, 1000000) AS gs;
    END IF;

    -- Table t_special_mixed: special values interleaved with regular values,
    -- so binary ops hit the special-value early-exit paths on roughly every row
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 't_special_mixed') THEN
        CREATE TABLE t_special_mixed(id bigserial primary key, order_date date, amount numeric);
        INSERT INTO t_special_mixed(order_date, amount) SELECT DATE '2024-01-01' + (gs % 730), CASE gs % 4 WHEN 0 THEN 'NaN'::numeric WHEN 1 THEN 'Infinity'::numeric WHEN 2 THEN '-Infinity'::numeric ELSE (gs % 9973) * 1.37 END FROM generate_series(1, 1000000) AS gs;
    END IF;
END $$;

CREATE SEQUENCE IF NOT EXISTS results_seq;

VACUUM ANALYZE t;
VACUUM ANALYZE t_big;
VACUUM ANALYZE t_small;
VACUUM ANALYZE t_special;
VACUUM ANALYZE t_special_mixed;

-- Target tables for COPY FROM benchmarks (numeric_recv / numeric_in).
-- Temp tables: no WAL, no persistent catalog entries.
DROP TABLE IF EXISTS t_copy_bin;
DROP TABLE IF EXISTS t_copy_txt;
CREATE TEMP TABLE t_copy_bin(amount numeric);
CREATE TEMP TABLE t_copy_txt(amount numeric);

CREATE OR REPLACE FUNCTION benchmark_queries(
    queries text[],
    num_workers integer DEFAULT 0,
    disable_sort boolean DEFAULT false)
RETURNS TABLE(
    query_text text,
    runtimes_ms numeric[]
) AS $$
DECLARE
    start_time timestamp;
    q text;
BEGIN
    EXECUTE format('SET LOCAL max_parallel_workers_per_gather = %s', num_workers);
    -- For hash benchmarks: force HashAggregate / hashed subplans / hash joins
    -- so hash_numeric is exercised instead of numeric_cmp/sortsupport paths
    EXECUTE format('SET LOCAL enable_sort = %s', NOT disable_sort);
    EXECUTE format('SET LOCAL enable_mergejoin = %s', NOT disable_sort);

    runtimes_ms := array_fill(0::numeric, ARRAY[5]);

    FOREACH q IN ARRAY queries
    LOOP
        FOR j IN 1..array_length(runtimes_ms, 1) LOOP
            start_time := clock_timestamp();
            EXECUTE q;
            runtimes_ms[j] := EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000;
        END LOOP;
        
        RETURN QUERY SELECT q, runtimes_ms;
        -- Average excludes the first run (cold cache/buffer warmup)
        RAISE WARNING 'Query completed: avg runtime = % ms', (SELECT min(v) FROM unnest(runtimes_ms[2:array_length(runtimes_ms, 1)]) AS v);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

\set branch `git rev-parse --abbrev-ref HEAD`
SELECT 'numeric_benchmark_results_' || regexp_replace(:'branch', '[^a-zA-Z0-9_]', '_', 'g') || '_' || nextval('results_seq') AS results_table \gset
\echo :results_table

CREATE TABLE :results_table (
    run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    query_text text,
    runtimes_ms numeric[]
);

INSERT INTO :results_table (query_text, runtimes_ms)
SELECT *
FROM benchmark_queries(ARRAY[
    -- Table t
    'select max(amount) from t',
    'select max(amount) from t where order_date > ''2025-01-01''',
    'select order_date, max(amount) from t group by order_date',
    'select order_date, max(amount) from t where order_date > ''2025-01-01'' group by order_date',
    'select min(amount) from t',
    'select min(amount) from t where order_date > ''2025-01-01''',
    'select order_date, min(amount) from t group by order_date',
    'select order_date, min(amount) from t where order_date > ''2025-01-01'' group by order_date',
    'select sum(amount) from t',
    'select sum(amount) from t where order_date > ''2025-01-01''',
    'select order_date, sum(amount) from t group by order_date',
    'select order_date, sum(amount) from t where order_date > ''2025-01-01'' group by order_date',
    'select avg(amount) from t',
    'select avg(amount) from t where order_date > ''2025-01-01''',
    'select order_date, avg(amount) from t group by order_date',
    'select order_date, avg(amount) from t where order_date > ''2025-01-01'' group by order_date',
    'select variance(amount) from t',
    'select variance(amount) from t where order_date > ''2025-01-01''',
    'select order_date, variance(amount) from t group by order_date',
    'select order_date, variance(amount) from t where order_date > ''2025-01-01'' group by order_date',
    'select max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t',
    'select max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t where order_date > ''2025-01-01''',
    'select order_date, max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t group by order_date',
    'select order_date, max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t where order_date > ''2025-01-01'' group by order_date',
    'select count(*) from t where amount = 12345.67',
    'select count(*) from t where amount <> 0',
    'select count(*) from t where amount > 10000',
    'select count(*) from t where amount >= 10000',
    'select count(*) from t where amount < 50000',
    'select count(*) from t where amount <= 50000',
    'select count(*) from t where amount > amount * 0.5',
    'select count(*) from t where amount + 100 > amount * 1.1',
    'select sum(amount + 100) from t',
    'select sum(amount - 50) from t',
    'select sum(amount * 2) from t',
    'select sum(amount / 3) from t',
    'select sum(amount % 10) from t',
    'select count(*) from t where amount * 2 > 100000',
    'select count(*) from t where amount / 10 > 5000',
    'select count(*) from t where amount + amount > 200000',
    'select sum((amount + 100) * 0.95) from t',
    'select sum(amount * 1.08 - amount * 0.03) from t',
    'select sum(abs(amount)) from t',
    'select sum(abs(amount - 50000)) from t',
    'select count(*) from t where abs(amount - 50000) < 10000',
    'select sum(round(amount, 0)) from t',
    'select sum(round(amount, 1)) from t',
    'select sum(trunc(amount, 0)) from t',
    'select sum(trunc(amount, 1)) from t',
    'select sum(floor(amount)) from t',
    'select sum(ceil(amount)) from t',
    'select sum(ceiling(amount)) from t',
    'select sum(sign(amount)) from t',
    'select count(*) from t where sign(amount) > 0',
    'select sum(power(amount, 2)) from t',
    'select sum(sqrt(abs(amount))) from t',
    --'select sum(power(amount, 0.5)) from t',
    'select sum(mod(amount, 100)) from t',
    'select count(*) from t where mod(amount, 10) = 0',
    'select count(*) from t where amount % 1000 < 100',
    'select count(*), width_bucket(amount, 0, 100000, 10) from t group by width_bucket(amount, 0, 100000, 10)',
    'select sum(round(abs(amount - 50000), 2)) from t',
    'select sum(floor(amount / 100) * 100) from t',
    'select sum(ceil(amount / 100) * 100) from t',
    'select sum(case when amount > 50000 then amount * 0.9 else amount * 1.1 end) from t',
    'select amount from t order by amount limit 1000',
    'select amount from t order by amount desc limit 1000',
    'select amount from t order by abs(amount - 50000) limit 1000',
    -- Table t_big
    'select max(amount) from t_big',
    'select max(amount) from t_big where order_date > ''2025-01-01''',
    'select order_date, max(amount) from t_big group by order_date',
    'select order_date, max(amount) from t_big where order_date > ''2025-01-01'' group by order_date',
    'select min(amount) from t_big',
    'select min(amount) from t_big where order_date > ''2025-01-01''',
    'select order_date, min(amount) from t_big group by order_date',
    'select order_date, min(amount) from t_big where order_date > ''2025-01-01'' group by order_date',
    'select sum(amount) from t_big',
    'select sum(amount) from t_big where order_date > ''2025-01-01''',
    'select order_date, sum(amount) from t_big group by order_date',
    'select order_date, sum(amount) from t_big where order_date > ''2025-01-01'' group by order_date',
    'select avg(amount) from t_big',
    'select avg(amount) from t_big where order_date > ''2025-01-01''',
    'select order_date, avg(amount) from t_big group by order_date',
    'select order_date, avg(amount) from t_big where order_date > ''2025-01-01'' group by order_date',
    'select variance(amount) from t_big',
    'select variance(amount) from t_big where order_date > ''2025-01-01''',
    'select order_date, variance(amount) from t_big group by order_date',
    'select order_date, variance(amount) from t_big where order_date > ''2025-01-01'' group by order_date',
    'select max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t_big',
    'select max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t_big where order_date > ''2025-01-01''',
    'select order_date, max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t_big group by order_date',
    'select order_date, max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t_big where order_date > ''2025-01-01'' group by order_date',
    'select count(*) from t_big where amount = 12345.67',
    'select count(*) from t_big where amount <> 0',
    'select count(*) from t_big where amount > 10000',
    'select count(*) from t_big where amount >= 10000',
    'select count(*) from t_big where amount < 50000',
    'select count(*) from t_big where amount <= 50000',
    'select count(*) from t_big where amount > amount * 0.5',
    'select count(*) from t_big where amount + 100 > amount * 1.1',
    'select sum(amount + 100) from t_big',
    'select sum(amount - 50) from t_big',
    'select sum(amount * 2) from t_big',
    'select sum(amount / 3) from t_big',
    'select sum(amount % 10) from t_big',
    'select count(*) from t_big where amount * 2 > 100000',
    'select count(*) from t_big where amount / 10 > 5000',
    'select count(*) from t_big where amount + amount > 200000',
    'select sum((amount + 100) * 0.95) from t_big',
    'select sum(amount * 1.08 - amount * 0.03) from t_big',
    'select sum(abs(amount)) from t_big',
    'select sum(abs(amount - 50000)) from t_big',
    'select count(*) from t_big where abs(amount - 50000) < 10000',
    'select sum(round(amount, 0)) from t_big',
    'select sum(round(amount, 1)) from t_big',
    'select sum(trunc(amount, 0)) from t_big',
    'select sum(trunc(amount, 1)) from t_big',
    'select sum(floor(amount)) from t_big',
    'select sum(ceil(amount)) from t_big',
    'select sum(ceiling(amount)) from t_big',
    'select sum(sign(amount)) from t_big',
    'select count(*) from t_big where sign(amount) > 0',
    'select sum(power(amount, 2)) from t_big',
    'select sum(sqrt(abs(amount))) from t_big',
    --'select sum(power(amount, 0.5)) from t_big',
    'select sum(mod(amount, 100)) from t_big',
    'select count(*) from t_big where mod(amount, 10) = 0',
    'select count(*) from t_big where amount % 1000 < 100',
    'select count(*), width_bucket(amount, 0, 100000, 10) from t_big group by width_bucket(amount, 0, 100000, 10)',
    'select sum(round(abs(amount - 50000), 2)) from t_big',
    'select sum(floor(amount / 100) * 100) from t_big',
    'select sum(ceil(amount / 100) * 100) from t_big',
    'select sum(case when amount > 50000 then amount * 0.9 else amount * 1.1 end) from t_big',
    'select amount from t_big order by amount limit 1000',
    'select amount from t_big order by amount desc limit 1000',
    'select amount from t_big order by abs(amount - 50000) limit 1000',
    -- Table t_small
    'select max(amount) from t_small',
    'select max(amount) from t_small where order_date > ''2025-01-01''',
    'select order_date, max(amount) from t_small group by order_date',
    'select order_date, max(amount) from t_small where order_date > ''2025-01-01'' group by order_date',
    'select min(amount) from t_small',
    'select min(amount) from t_small where order_date > ''2025-01-01''',
    'select order_date, min(amount) from t_small group by order_date',
    'select order_date, min(amount) from t_small where order_date > ''2025-01-01'' group by order_date',
    'select sum(amount) from t_small',
    'select sum(amount) from t_small where order_date > ''2025-01-01''',
    'select order_date, sum(amount) from t_small group by order_date',
    'select order_date, sum(amount) from t_small where order_date > ''2025-01-01'' group by order_date',
    'select avg(amount) from t_small',
    'select avg(amount) from t_small where order_date > ''2025-01-01''',
    'select order_date, avg(amount) from t_small group by order_date',
    'select order_date, avg(amount) from t_small where order_date > ''2025-01-01'' group by order_date',
    'select variance(amount) from t_small',
    'select variance(amount) from t_small where order_date > ''2025-01-01''',
    'select order_date, variance(amount) from t_small group by order_date',
    'select order_date, variance(amount) from t_small where order_date > ''2025-01-01'' group by order_date',
    'select max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t_small',
    'select max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t_small where order_date > ''2025-01-01''',
    'select order_date, max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t_small group by order_date',
    'select order_date, max(amount), min(amount), sum(amount), avg(amount), variance(amount) from t_small where order_date > ''2025-01-01'' group by order_date',
    'select count(*) from t_small where amount = 123.45',
    'select count(*) from t_small where amount <> 0',
    'select count(*) from t_small where amount > 100',
    'select count(*) from t_small where amount >= 100',
    'select count(*) from t_small where amount < 500',
    'select count(*) from t_small where amount <= 500',
    'select count(*) from t_small where amount > amount * 0.5',
    'select count(*) from t_small where amount + 10 > amount * 1.1',
    'select sum(amount + 10) from t_small',
    'select sum(amount - 5) from t_small',
    'select sum(amount * 2) from t_small',
    'select sum(amount / 3) from t_small',
    'select sum(amount % 10) from t_small',
    'select count(*) from t_small where amount * 2 > 1000',
    'select count(*) from t_small where amount / 10 > 50',
    'select count(*) from t_small where amount + amount > 2000',
    'select sum((amount + 10) * 0.95) from t_small',
    'select sum(amount * 1.08 - amount * 0.03) from t_small',
    'select sum(abs(amount)) from t_small',
    'select sum(abs(amount - 500)) from t_small',
    'select count(*) from t_small where abs(amount - 500) < 100',
    'select sum(round(amount, 0)) from t_small',
    'select sum(round(amount, 1)) from t_small',
    'select sum(trunc(amount, 0)) from t_small',
    'select sum(trunc(amount, 1)) from t_small',
    'select sum(floor(amount)) from t_small',
    'select sum(ceil(amount)) from t_small',
    'select sum(ceiling(amount)) from t_small',
    'select sum(sign(amount)) from t_small',
    'select count(*) from t_small where sign(amount) > 0',
    'select sum(power(amount, 2)) from t_small',
    'select sum(sqrt(abs(amount))) from t_small',
    --'select sum(power(amount, 0.5)) from t_small',
    'select sum(mod(amount, 100)) from t_small',
    'select count(*) from t_small where mod(amount, 10) = 0',
    'select count(*) from t_small where amount % 1000 < 100',
    'select count(*), width_bucket(amount, 0, 1000, 10) from t_small group by width_bucket(amount, 0, 1000, 10)',
    'select sum(round(abs(amount - 500), 2)) from t_small',
    'select sum(floor(amount / 100) * 100) from t_small',
    'select sum(ceil(amount / 100) * 100) from t_small',
    'select sum(case when amount > 500 then amount * 0.9 else amount * 1.1 end) from t_small',
    'select amount from t_small order by amount limit 1000',
    'select amount from t_small order by amount desc limit 1000',
    'select amount from t_small order by abs(amount - 500) limit 1000',
    -- Additional function coverage (stddev/variance aggregates, unary ops,
    -- div/gcd/lcm/factorial, exp/ln/log, scale/min_scale/trim_scale,
    -- casts to/from int2/int4/int8/float4/float8, generate_series, in_range)
    'select stddev(amount) from t',
    'select stddev_samp(amount) from t',
    'select stddev_pop(amount) from t',
    'select var_samp(amount) from t',
    'select var_pop(amount) from t',
    'select stddev(amount) from t_big',
    'select var_samp(amount) from t_big',
    'select var_pop(amount) from t_big',
    'select stddev(amount) from t_small',
    'select var_samp(amount) from t_small',
    'select var_pop(amount) from t_small',
    'select order_date, stddev(amount), var_samp(amount), var_pop(amount) from t group by order_date',
    'select sum(-amount) from t',
    'select sum(+amount) from t',
    'select sum(-amount) from t_big',
    'select sum(-amount) from t_small',
    'select sum(div(amount, 7)) from t',
    'select sum(div(amount, 7)) from t_big',
    'select sum(div(amount, 7)) from t_small',
    'select sum(gcd(mod(amount, 1000), 30)) from t',
    'select sum(gcd(mod(amount, 1000), 30)) from t_big',
    'select sum(gcd(mod(amount, 100), 30)) from t_small',
    'select sum(lcm(mod(amount, 100), 12)) from t',
    'select sum(lcm(mod(amount, 100), 12)) from t_big',
    'select sum(lcm(mod(amount, 100), 12)) from t_small',
    'select sum(factorial(trunc(mod(amount, 20))::int8)) from t',
    'select sum(factorial(trunc(mod(amount, 20))::int8)) from t_big',
    'select sum(factorial(trunc(mod(amount, 20))::int8)) from t_small',
    'select sum(ln(abs(amount) + 1)) from (select amount from t limit 50000) s',
    'select sum(ln(abs(amount) + 1)) from (select amount from t_big limit 50000) s',
    'select sum(ln(abs(amount) + 1)) from (select amount from t_small limit 50000) s',
    'select sum(log(abs(amount) + 1)) from (select amount from t limit 15000) s',
    'select sum(log(2, abs(amount) + 1)) from (select amount from t limit 15000) s',
    'select sum(exp(mod(amount, 5))) from (select amount from t limit 100000) s',
    'select sum(exp(mod(amount, 5))) from (select amount from t_big limit 100000) s',
    'select sum(exp(mod(amount, 5))) from (select amount from t_small limit 100000) s',
    'select sum(scale(amount)) from t',
    'select sum(min_scale(amount)) from t',
    'select sum(scale(trim_scale(amount))) from t',
    'select sum(min_scale(amount)) from t_big',
    'select sum(scale(amount)) from t_small',
    'select sum(amount::int8) from t',
    'select sum(amount::int4) from t_small',
    'select sum((amount % 100)::int2) from t_small',
    'select sum((amount % 100)::int2::numeric) from t_small',
    'select sum(amount::float8) from t',
    'select sum(amount::float8) from t_big',
    'select sum((amount % 1000)::float4) from t',
    'select sum((amount % 1000)::float4::numeric) from t',
    'select sum((id % 1000000)::numeric) from t',
    'select count(*) from generate_series(1::numeric, 500000) g',
    'select count(*) from generate_series(1::numeric, 500000, 2.5) g',
    'select count(*) from (select sum(amount) over (order by amount range between 10 preceding and 10 following) from t_small) s',
    -- Hashing (direct calls; see also results_hash_* below for
    -- HashAggregate / hash join coverage)
    'select sum(hash_numeric(amount)) from t',
    'select sum(hash_numeric(amount)) from t_big',
    'select sum(hash_numeric(amount)) from t_small',
    'select sum(hash_numeric(amount)) from t_special',
    'select sum(hash_numeric_extended(amount, 42)) from t',
    'select sum(hash_numeric_extended(amount, 42)) from t_big',
    'select sum(hash_numeric_extended(amount, 42)) from t_small',
    'select sum(hash_numeric_extended(amount, 42)) from t_special',
    -- Table t_special (NaN / Infinity / -Infinity only)
    'select max(amount) from t_special',
    'select min(amount) from t_special',
    'select sum(amount) from t_special',
    'select avg(amount) from t_special',
    'select variance(amount) from t_special',
    'select stddev(amount) from t_special',
    'select var_samp(amount) from t_special',
    'select order_date, max(amount), min(amount), sum(amount), avg(amount) from t_special group by order_date',
    'select count(*) from t_special where amount = ''NaN''',
    'select count(*) from t_special where amount <> ''NaN''',
    'select count(*) from t_special where amount > 0',
    'select count(*) from t_special where amount < 0',
    'select count(*) from t_special where amount >= ''Infinity''',
    'select count(*) from t_special where amount <= ''-Infinity''',
    'select count(*) from t_special where amount = amount',
    'select count(*) from t_special where amount > amount * 0.5',
    'select sum(amount + 100) from t_special',
    'select sum(amount - 50) from t_special',
    'select sum(amount * 2) from t_special',
    'select sum(amount / 3) from t_special',
    'select sum(amount % 10) from t_special',
    'select sum(amount + amount) from t_special',
    'select sum(amount * amount) from t_special',
    'select sum(-amount) from t_special',
    'select sum(abs(amount)) from t_special',
    'select sum(sign(amount)) from t_special',
    'select count(*) from t_special where sign(amount) > 0',
    'select sum(round(amount, 1)) from t_special',
    'select sum(trunc(amount, 1)) from t_special',
    'select sum(floor(amount)) from t_special',
    'select sum(ceil(amount)) from t_special',
    'select sum(ln(abs(amount))) from t_special',
    'select sum(min_scale(amount)) from t_special',
    'select sum(scale(trim_scale(amount))) from t_special',
    'select sum(amount::float8) from t_special',
    'select amount from t_special order by amount limit 1000',
    'select amount from t_special order by amount desc limit 1000',
    -- Table t_special_mixed (special values + regular values)
    'select max(amount) from t_special_mixed',
    'select min(amount) from t_special_mixed',
    'select sum(amount) from t_special_mixed',
    'select avg(amount) from t_special_mixed',
    'select variance(amount) from t_special_mixed',
    'select stddev(amount) from t_special_mixed',
    'select var_samp(amount) from t_special_mixed',
    'select order_date, max(amount), min(amount), sum(amount), avg(amount) from t_special_mixed group by order_date',
    'select count(*) from t_special_mixed where amount = ''NaN''',
    'select count(*) from t_special_mixed where amount <> ''NaN''',
    'select count(*) from t_special_mixed where amount > 100',
    'select count(*) from t_special_mixed where amount < 100',
    'select count(*) from t_special_mixed where amount = 500',
    'select count(*) from t_special_mixed where amount > amount * 0.5',
    'select sum(amount + 100) from t_special_mixed',
    'select sum(amount - 50) from t_special_mixed',
    'select sum(amount * 2) from t_special_mixed',
    'select sum(amount / 3) from t_special_mixed',
    'select sum(amount % 10) from t_special_mixed',
    'select sum(amount + amount) from t_special_mixed',
    'select sum(amount * amount) from t_special_mixed',
    'select sum(-amount) from t_special_mixed',
    'select sum(abs(amount)) from t_special_mixed',
    'select sum(sign(amount)) from t_special_mixed',
    'select count(*) from t_special_mixed where sign(amount) > 0',
    'select sum(round(amount, 1)) from t_special_mixed',
    'select sum(trunc(amount, 1)) from t_special_mixed',
    'select sum(floor(amount)) from t_special_mixed',
    'select sum(ceil(amount)) from t_special_mixed',
    'select sum(ln(abs(amount) + 1)) from t_special_mixed',
    'select sum(exp(mod(abs(amount), 5))) from t_special_mixed',
    'select sum(min_scale(amount)) from t_special_mixed',
    'select sum(scale(trim_scale(amount))) from t_special_mixed',
    'select sum(gcd(mod(abs(amount), 1000), 30)) from t_special_mixed',
    'select sum(amount::float8) from t_special_mixed',
    'select sum(div(amount, 7)) from t_special_mixed',
    'select sum(mod(amount, 100)) from t_special_mixed',
    'select amount from t_special_mixed order by amount limit 1000',
    'select amount from t_special_mixed order by amount desc limit 1000',
    -- Moving-frame window aggregates (exercises *_accum_inv inverse
    -- transition functions: numeric_accum_inv, int8_avg_accum_inv, etc.)
    'select count(*) from (select sum(amount) over w, avg(amount) over w from t window w as (order by id rows between 1000 preceding and current row)) s',
    'select count(*) from (select sum(amount) over w, avg(amount) over w from t_small window w as (order by id rows between 1000 preceding and current row)) s',
    'select count(*) from (select variance(amount) over w from t window w as (order by id rows between 1000 preceding and current row)) s',
    -- I/O functions: numeric_out (text cast), numeric_in (text cast / COPY),
    -- numeric_send (binary COPY TO), numeric_recv (binary COPY FROM).
    -- The COPY statements require superuser or pg_read/write_server_files.
    'select sum(length(amount::text)) from t',
    'select sum(length(amount::text)) from t_big',
    'select sum(length(amount::text)) from t_small',
    'select sum(((id % 1000) + 0.5)::text::numeric) from t',
    'select sum(((id % 1000) + 0.5)::text::numeric) from t_big',
    'select sum(((id % 1000) + 0.5)::text::numeric) from t_small',
    'COPY (SELECT amount FROM t) TO ''/dev/null'' WITH (FORMAT binary)',
    'COPY (SELECT amount FROM t_big) TO ''/dev/null'' WITH (FORMAT binary)',
    'COPY (SELECT amount FROM t_small) TO ''/dev/null'' WITH (FORMAT binary)',
    'COPY t_copy_bin(amount) FROM ''/tmp/numeric_bench.bin'' WITH (FORMAT binary)',
    'COPY t_copy_txt(amount) FROM ''/tmp/numeric_bench.txt'''
]);

-- Parallel aggregation exercises the *_combine, *_serialize and
-- *_deserialize functions (partial aggregation serializes transition states
-- for the gather). Requires a build/config that allows parallel workers.
-- Appended to the main results table with continued query numbering.
INSERT INTO :results_table (query_text, runtimes_ms)
SELECT *
FROM benchmark_queries(ARRAY[
    'select sum(amount) from t',
    'select avg(amount) from t',
    'select variance(amount) from t',
    'select stddev(amount) from t',
    'select sum(amount) from t_big',
    'select avg(amount) from t_big',
    'select variance(amount) from t_big',
    'select sum(amount) from t_small',
    'select avg(amount) from t_small',
    'select variance(amount) from t_small',
    'select sum(amount) from t_special_mixed',
    'select avg(amount) from t_special_mixed',
    'select order_date, sum(amount), avg(amount) from t group by order_date'
], 4);

-- Hash benchmarks: sort and merge join are disabled so the planner picks
-- HashAggregate / hashed subplans / hash joins, exercising hash_numeric
-- (and hash_numeric_extended for extended hash ops). These run separately
-- because disabling sort would skew the ORDER BY benchmarks in the main run.
INSERT INTO :results_table (query_text, runtimes_ms)
SELECT *
FROM benchmark_queries(ARRAY[
    'select count(*) from (select distinct amount from t) s',
    'select count(*) from (select distinct amount from t_big) s',
    'select count(*) from (select distinct amount from t_small) s',
    'select count(*) from (select distinct amount from t_special) s',
    'select amount, count(*) from t_small group by amount',
    'select amount, count(*) from t_special group by amount',
    'select count(*) from t a join t_small b on a.amount = b.amount',
    'select count(*) from t_small where amount in (select amount from t_special_mixed)'
], 0, true);

-- select m.run_id, m.query_text, b.ms as before_ms, a.ms as after_ms, trunc(b.ms / a.ms, 2) as speedup
-- from numeric_benchmark_results_1 m
-- join numeric_benchmark_results_2 o on o.run_id = m.run_id
-- cross join lateral (select min(v) as ms from unnest(m.runtimes_ms) v) b
-- cross join lateral (select min(v) as ms from unnest(o.runtimes_ms) v) a
-- order by speedup;