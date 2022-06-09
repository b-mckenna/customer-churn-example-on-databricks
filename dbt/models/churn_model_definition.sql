{{
  config(
    materialized = 'view',
		meta = {
			"continual": {
				'type': 'Model',
				'name': 'user_churn',
				'index': 'msno',
				'time_index': 'ts',
				'target': 'is_churn',
				'split': 'split',
				'columns': [
					{'name' : 'msno', 'type': 'text', 'entity': 'kkbox_user'},
				],
				'exclude_columns': ['next_transaction', 'most_recent_transaction', 'days_expired'],
				'train': {
					'metric' : 'roc_auc',
					'excluded_model_types' : ['FastAI', 'KNN', 'NeuralNet', 'LightGBMLarge'],
					'size' : 'large',
				},
				},

		}
  )
}}

with t as (
	select
		msno,
		ts,
		lag(transaction_dt) over (partition by msno order by transaction_dt desc) as next_transaction,
		first_value(transaction_dt) over (partition by msno order by transaction_dt desc) as most_recent_transaction,
		datediff(days,transaction_dt,expiration_dt) as days_since_last_transaction,
		datediff(days,expiration_dt,date('{{ var("todays_date")}}')) as days_expired, --second date here is just "today's date". For this problem, the most recent month is 4/2017
		case
		       when (next_transaction is NULL and days_expired> {{ var("churn_threshold") }}) then True --no next transaction, if you are more than 30 days expired then you are churn
		       when (next_transaction is not NULL and datediff(days,expiration_dt,date(next_transaction,'YYYY-MM-DD'))>{{ var("churn_threshold") }}) then True --there is a next transaction, but you waited more than 30 days to renew, so this is churn
		       else False
		end as is_churn,
		--case
		--       when (is_churn=True and next_transaction is not NULL and most_recent_transaction > expiration_dt) then True --there was churn, but then another transaction after the expiration date.
		--       when (is_churn=False) then NULL --no churn, so not relevant
		--       else False --churn and no return
		--end as churn_and_return,
		case
			when ts < '{{ var("train_end") }}' and ts >= '{{ var("train_start") }}'  then 'TRAIN'
			when ts < '{{ var("vali_end") }}'  and ts >= '{{ var("vali_start") }}'  then 'VALI'
			when ts < '{{ var("test_end") }} ' and ts >= '{{ var("test_start") }}'  then 'TEST'
			else 'PREDICT_ME'
		end as SPLIT,
		datediff(days,(select max(log_date) from {{ ref('user_logs_all') }} where msno=A.msno and log_date<=A.ts),ts) as days_since_last_log
	from {{ ref('transactions_final') }} as A
)
select * from t
where ts >= '{{ var("train_start") }}'
and ts <= '{{ var("todays_date") }}'
