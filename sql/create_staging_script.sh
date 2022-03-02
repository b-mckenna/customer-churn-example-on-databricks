#!/bin/bash  

fname="snowsql_staging.sql"

split -l 5000000 user_logs.csv user_logs_small.csv
echo "user_logs.csv has been split into smaller files"

echo "
USE database KKBOX;

CREATE STAGE IF NOT EXISTS customer_churn;

" > $fname

echo "
PUT file://"${PWD}"/transactions.csv @customer_churn;
PUT file://"${PWD}"/transactions_v2.csv @customer_churn;
PUT file://"${PWD}"/members_v3.csv @customer_churn;
PUT file://"${PWD}"/user_logs_v2.csv @customer_churn;
" >> $fname

for i in $(find . -name 'user_logs_sm*')
do
 	echo "PUT file://"${PWD}${i:1}" @customer_churn;" >> $fname
done

for i in $(find . -name 'user_logs_sm*')
do
    fileArray+=("'"${i:2}".gz'")
	fileArray+=(", ")
done

echo "
COPY INTO KKBOX.CHURN.TRANSACTIONS FROM @customer_churn files = ('transactions.csv.gz', 'transactions_v2.csv.gz') file_format = (type = CSV skip_header = 1);
COPY INTO KKBOX.CHURN.MEMBERS FROM @customer_churn files = ('members_v3.csv.gz') file_format = (type = CSV skip_header = 1);
COPY INTO KKBOX.CHURN.MEMBERS FROM @customer_churn files = ('user_logs_v2.csv.gz') file_format = (type = CSV skip_header = 1);

COPY INTO KKBOX.CHURN.USER_LOGS FROM @customer_churn files = ("${fileArray[@]:0:19}") file_format = (type = CSV skip_header = 1);
COPY INTO KKBOX.CHURN.USER_LOGS FROM @customer_churn files = ("${fileArray[@]:20:19}") file_format = (type = CSV skip_header = 1);
COPY INTO KKBOX.CHURN.USER_LOGS FROM @customer_churn files = ("${fileArray[@]:40:19}") file_format = (type = CSV skip_header = 1);
COPY INTO KKBOX.CHURN.USER_LOGS FROM @customer_churn files = ("${fileArray[@]:60:19}") file_format = (type = CSV skip_header = 1);
COPY INTO KKBOX.CHURN.USER_LOGS FROM @customer_churn files = ("${fileArray[@]:80:19}") file_format = (type = CSV skip_header = 1);
COPY INTO KKBOX.CHURN.USER_LOGS FROM @customer_churn files = ("${fileArray[@]:100:19}") file_format = (type = CSV skip_header = 1);
COPY INTO KKBOX.CHURN.USER_LOGS FROM @customer_churn files = ("${fileArray[@]:120:19}") file_format = (type = CSV skip_header = 1);
COPY INTO KKBOX.CHURN.USER_LOGS FROM @customer_churn files = ("${fileArray[@]:140:17}") file_format = (type = CSV skip_header = 1);

" >> $fname

echo "
SELECT * FROM KKBOX.CHURN.TRANSACTIONS LIMIT 10;
SELECT * FROM KKBOX.CHURN.USER_LOGS LIMIT 10;
SELECT * FROM KKBOX.CHURN.MEMBERS LIMIT 10;

DROP STAGE IF EXISTS customer_churn; 
" >> $fname
echo "snowsql_staging.sql has been created. Finishing..."

find . -type f -name user_logs_small\* -exec rm -f {} \;

echo "user_logs_small files have been removed"
echo "Finished"