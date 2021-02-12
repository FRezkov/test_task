-- Create source schema
create schema if not exists raw_data;

create  table raw_data.tb_users (
uid uuid, 
registration_date timestamp without time zone, 
country varchar(128),
CONSTRAINT tb_users_pk PRIMARY KEY(uid)
); commit;

create table raw_data.tb_logins (
user_uid uuid, 
login varchar(64), 
account_type varchar(8),
CONSTRAINT tb_logins_pk PRIMARY KEY(login)
); commit;

create table raw_data.tb_operations (
operation_type varchar(36), 
operation_date timestamp without time zone, 
login varchar(36), 
amount numeric,
CONSTRAINT tb_operations_pk PRIMARY KEY(login, operation_type, operation_date)
); commit;

create table raw_data.tb_orders (
login varchar(36),
order_close_date timestamp without time zone, 
CONSTRAINT tb_orders_pk PRIMARY KEY(login, order_close_date)
); commit;

-- Populating the tables with test data
insert into raw_data.tb_users
select md5(RANDOM()::TEXT)::uuid as uid, 
       NOW() - '1 hours'::INTERVAL * (RANDOM()* 2500)::int -  '1 seconds'::INTERVAL * (RANDOM()* 100)::int as registration_date,
       CASE WHEN RANDOM() < 0.1 THEN 'Canada' 
            WHEN RANDOM() < 0.2 THEN 'Cyprus' 
            WHEN RANDOM() < 0.3 THEN 'Egypt' 
            WHEN RANDOM() < 0.4 THEN 'Georgia' 
            WHEN RANDOM() < 0.5 THEN 'Indonesia' 
            WHEN RANDOM() < 0.6 THEN 'Kenya' 
            WHEN RANDOM() < 0.7 THEN 'Brazil' 
            WHEN RANDOM() < 0.8 THEN 'France' 
            WHEN RANDOM() < 0.9 THEN 'Australia'
            ELSE 'Great Britan' end as country
  from generate_series(1,100000) gs 
; commit;
 

insert into raw_data.tb_logins
select user_uid, 
       unnest(ar_login) as login, 
       account_type
  from (
select u.uid as user_uid, 
       (array_agg(md5(RANDOM()::TEXT)))[:((RANDOM()* 10)/2)::int] as ar_login,
       CASE WHEN RANDOM() < 0.5 THEN 'real'
            ELSE 'demo' end as account_type
  from generate_series(1,5) gs
  join raw_data.tb_users u on 1=1
  group by 1,3
) t; commit;


insert into raw_data.tb_operations
with main as (
select CASE WHEN RANDOM() > 0.6 THEN 'withdrawal'
            ELSE 'deposit' end as operation_type,
       NOW() - '1 hours'::INTERVAL * (RANDOM()* 2500)::int -  '1 seconds'::INTERVAL * (RANDOM()* 100)::int as operation_date, 
       l.login,
       CASE WHEN RANDOM() > 0.6 THEN (RANDOM()* 2000)
            ELSE (RANDOM()* 2500) end as amount, 
       u.registration_date
  from raw_data.tb_logins l
  join raw_data.tb_users u on l.user_uid = u.uid 
  join (select 1 from generate_series(1,5)) gs on 1=1
 where l.account_type = 'real'
 order by l.login
), q1 as ( select operation_type,
                  operation_date,
                  login,
                  amount,
                  array_agg(operation_type) over(partition by login order by operation_date) as arr_rn
    from main
    where registration_date < operation_date
    ) select operation_type,
             operation_date,
             login,
             min(amount) as amount
        from q1 
       where coalesce(array_position(arr_rn, 'deposit'), 999) = 1
       group by operation_type, operation_date, login
;commit;


insert into raw_data.tb_orders
select distinct login,  order_close_date
  from (
select l.login,
       NOW() - '1 hours'::INTERVAL * (RANDOM()* 2500)::int -  '1 seconds'::INTERVAL * (RANDOM()* 100)::int as order_close_date,
       coalesce(t.operation_date, NOW() - '1 hours'::INTERVAL * (RANDOM()* 2500)::int -  '1 seconds'::INTERVAL * (RANDOM()* 100)::int) as operation_date
  from raw_data.tb_logins l
  left join raw_data.tb_operations t on l.login = t.login and t.operation_type = 'deposit'
  join (select 1 from generate_series(1,5)) gs on 1=1
) t 
where order_close_date >  operation_date 
; commit;

