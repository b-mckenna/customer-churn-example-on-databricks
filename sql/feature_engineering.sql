create or replace view kkbox.members_all as(
	select
	 msno,
	 city,
	 bd,
	 gender,
	 registered_via,
	 to_date(cast(registration_init_time as string),'yyyyMMDD') as registration_dt
 from kkbox.members
);

create or replace view kkbox.transactions_all as(
  with t as(
    select distinct
		msno,
		to_date(cast(transaction_date as string),'yyyyMMDD') as transaction_dt,
		to_date(cast(membership_expire_date as string),'yyyyMMDD') as expiration_dt,
		payment_method_id,
		payment_plan_days,
		plan_list_price,
		actual_amount_paid,
		is_auto_renew,
		is_cancel
    from kkbox.transactions
  )
  select *,
  timestamp(concat(string(year(expiration_dt)) + string(month(expiration_dt)) + "1")) as ts --predict churn in the beginning of the  month when account is expiring
  from t
);

create or replace view kkbox.transactions_canceled as(
	with t as (
	  select
			A.msno,
			A.transaction_dt,
			max(A.ts) as ts,
			max(B.transaction_dt) as most_recent_transaction_dt
	  from kkbox.transactions_all as A
	  join kkbox.transactions_all as B
	  where B.msno = A.msno
	  and B.transaction_dt <= A.ts
		and B.transaction_dt > A.transaction_dt
	  group by A.msno, A.transaction_dt
	  order by A.msno, A.transaction_dt
	)
	select
		t.msno,
		t.transaction_dt,
		max(t.ts) as ts,
		max(t.most_recent_transaction_dt) as most_recent_transaction_dt,
		max(c.is_cancel) as most_recent_transaction_cancel
	from t
	join kkbox.transactions_all as C
	where C.msno = t.msno
	and C.transaction_dt = t.most_recent_transaction_dt
	group by t.msno, t.transaction_dt
);

create or replace view kkbox.transactions_final as(
	with t as (
	 select A.*,
	  coalesce(B.most_recent_transaction_cancel, 0) as most_recent_transaction_cancel
	  from kkbox.transactions_all  as A
	  left join kkbox.transactions_canceled  as B
	  on A.msno = B.msno
	  and A.transaction_dt = B.transaction_dt
	  where A.is_cancel = 0
	  order by A.msno, A.transaction_dt
	)
	select
	  msno, ts,
	  max(payment_method_id) as payment_method_id,
	  max(payment_plan_days) as payment_plan_days,
	  sum(plan_list_price) as plan_list_price,
	  sum(actual_amount_paid) as actual_amount_paid,
	  max(is_auto_renew) as is_auto_renew,
		max(expiration_dt) as expiration_dt,
		max(transaction_dt) as transaction_dt,
	  max(most_recent_transaction_cancel) as most_recent_transaction_cancel
	  from t
	  group by msno, ts
		--squash sub-montly subs all into one month, use last value for exp/trans dt to calculate churn
);

create or replace view kkbox.user_logs_all as(
  with d as(
  select distinct *,
    to_date(cast(date as string),'yyyyMMDD') as log_date
	from kkbox.user_logs
  )
  select *,
  timestamp(log_date) as ts
  from d
);

create table user_logs_agg(
	with log_matrix as (
	  select B.msno, A.ts from
	  (select distinct ts from kkbox.user_logs_all) as A
	  cross join
	  (select distinct msno from kkbox.user_logs_all) as B
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
	kkbox.user_logs_all as t
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
);