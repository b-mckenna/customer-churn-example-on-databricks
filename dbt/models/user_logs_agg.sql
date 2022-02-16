{{
  config(
    materialized = 'table',
		meta = {
			"continual": {
				'type': 'FeatureSet',
				'name': 'user_logs_agg',
				'entity': 'kkbox_user',
				'index': 'msno',
				'time_index': 'ts',
				}
		}
  )
}}

with log_matrix as (
  select B.msno, A.ts from
  (select distinct ts from {{ ref('user_logs_all') }}) as A
  cross join
  (select distinct msno from {{ ref('user_logs_all') }}) as B
),
logs_full as (
select
    log_matrix.msno,
    log_matrix.ts,
    t.num_25,
    t.num_50,
    t.num_75,
    t.num_985,
    t.num_100,
    t.num_unq,
    t.total_secs
from log_matrix
left join
{{ ref('user_logs_all') }}) as t
on log_matrix.msno = t.msno
  and log_matrix.ts = t.ts
)
select
msno,
ts,
count_if(total_secs > 0) over (partition by msno order by ts desc rows between 1 following and 30 following) as count_active_30_day,
count_if(total_secs > 0) over (partition by msno order by ts desc rows between 1 following and 90 following) as count_active_90_day,
count_if(total_secs > 0) over (partition by msno order by ts desc rows between 1 following and 180 following) as count_active_180_day,
avg(num_25) over (partition by msno order by ts desc rows between 1 following and 30 following) as num_25_30_day_avg,
avg(num_25) over (partition by msno order by ts desc rows between 1 following and 90 following) as num_25_60_day_avg,
avg(num_25) over (partition by msno order by ts desc rows between 1 following and 180 following) as num_25_180_day_avg,
avg(num_50) over (partition by msno order by ts desc rows between 1 following and 30 following) as num_50_30_day_avg,
avg(num_50) over (partition by msno order by ts desc rows between 1 following and 90 following) as num_50_60_day_avg,
avg(num_50) over (partition by msno order by ts desc rows between 1 following and 180 following) as num_50_180_day_avg,
avg(num_75) over (partition by msno order by ts desc rows between 1 following and 30 following) as num_75_30_day_avg,
avg(num_75) over (partition by msno order by ts desc rows between 1 following and 90 following) as num_75_60_day_avg,
avg(num_75) over (partition by msno order by ts desc rows between 1 following and 180 following) as num_75_180_day_avg,
avg(num_985) over (partition by msno order by ts desc rows between 1 following and 30 following) as num_985_30_day_avg,
avg(num_985) over (partition by msno order by ts desc rows between 1 following and 90 following) as num_985_60_day_avg,
avg(num_985) over (partition by msno order by ts desc rows between 1 following and 180 following) as num_985_180_day_avg,
avg(num_100) over (partition by msno order by ts desc rows between 1 following and 30 following) as num_100_30_day_avg,
avg(num_100) over (partition by msno order by ts desc rows between 1 following and 90 following) as num_100_60_day_avg,
avg(num_100) over (partition by msno order by ts desc rows between 1 following and 180 following) as num_100_180_day_avg,
avg(num_unq) over (partition by msno order by ts desc rows between 1 following and 30 following) as num_unq_30_day_avg,
avg(num_unq) over (partition by msno order by ts desc rows between 1 following and 90 following) as num_unq_60_day_avg,
avg(num_unq) over (partition by msno order by ts desc rows between 1 following and 180 following) as num_unq_180_day_avg,
avg(total_secs) over (partition by msno order by ts desc rows between 1 following and 30 following) as total_secs_30_day_avg,
avg(total_secs) over (partition by msno order by ts desc rows between 1 following and 90 following) as total_secs_60_day_avg,
avg(total_secs) over (partition by msno order by ts desc rows between 1 following and 180 following) as total_secs_180_day_avg
from logs_full
