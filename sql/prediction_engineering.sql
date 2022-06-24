create or replace view kkbox.churn_model_definition as(
	with n as (
		select
			msno,
			ts,
            expiration_dt,
			lag(transaction_dt) over (partition by msno order by transaction_dt desc) as next_transaction,
			first_value(transaction_dt) over (partition by msno order by transaction_dt desc) as most_recent_transaction,
			datediff(transaction_dt,expiration_dt) as days_since_last_transaction,
			datediff(expiration_dt,date('2017-04-01')) as days_expired --second date here is just "today's date". For this problem, the most recent month is 4/2017
            --datediff((select max(log_date) from kkbox.user_logs_all where msno=A.msno and log_date<=A.ts),ts) as days_since_last_log
		from kkbox.transactions_final as A
	),
    t as(
      select 
      *, 
      case
        when (next_transaction is NULL and days_expired>30) then True --no next transaction, if you are more than 30 days expired then you are churn
		when (next_transaction is not Null and datediff(expiration_dt,to_date(cast(next_transaction as string),'yyyy-MM-DD'))>30) then True --there is a next transaction, but you waited more than 30 days to renew, so this is churn
		else False
      end as is_churn,
      case
        when ts < '2017-01-01' and ts >= '2016-06-01' then 'TRAIN'
		when ts < '2017-02-01' and ts >= '2017-01-01' then 'VALI'
		when ts < '2017-03-01' and ts >= '2017-02-01' then 'TEST'
        else 'PREDICT_ME'
      end as SPLIT
      from n
    )
	select * from t
	where ts >= '2016-06-01'
	and ts <= '2017-04-01'
);