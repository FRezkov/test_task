-- Create a database structure

create  table tb_users (
uid uuid, 
registration_date timestamp without time zone, 
country varchar(128),
CONSTRAINT tb_users_pk PRIMARY KEY(uid)
); commit;

create table tb_logins (
user_uid uuid, 
login varchar(64), 
account_type varchar(8),
CONSTRAINT tb_logins_pk PRIMARY KEY(login)
); commit;

create table tb_operations (
operation_type varchar(36), 
operation_date timestamp without time zone, 
login varchar(36), 
amount numeric,
CONSTRAINT tb_operations_pk PRIMARY KEY(login, operation_type, operation_date)
); commit;

create table tb_orders (
login varchar(36),
order_close_date timestamp without time zone, 
CONSTRAINT tb_orders_pk PRIMARY KEY(login, order_close_date)
); commit;

-- Populating the tables with test data
insert into tb_users
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
 

insert into tb_logins
select user_uid, 
       unnest(ar_login) as login, 
       account_type
  from (
select u.uid as user_uid, 
       (array_agg(md5(RANDOM()::TEXT)))[:((RANDOM()* 10)/2)::int] as ar_login,
       CASE WHEN RANDOM() < 0.5 THEN 'real'
            ELSE 'demo' end as account_type
  from generate_series(1,5) gs
  join tb_users u on 1=1
  group by 1,3
) t; commit;


insert into tb_operations
with main as (
select CASE WHEN RANDOM() > 0.6 THEN 'withdrawal'
            ELSE 'deposit' end as operation_type,
       NOW() - '1 hours'::INTERVAL * (RANDOM()* 2500)::int -  '1 seconds'::INTERVAL * (RANDOM()* 100)::int as operation_date, 
       l.login,
       CASE WHEN RANDOM() > 0.6 THEN (RANDOM()* 2000)
            ELSE (RANDOM()* 2500) end as amount, 
       u.registration_date
  from tb_logins l
  join tb_users u on l.user_uid = u.uid 
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


insert into tb_orders
select distinct login,  order_close_date
  from (
select l.login,
       NOW() - '1 hours'::INTERVAL * (RANDOM()* 2500)::int -  '1 seconds'::INTERVAL * (RANDOM()* 100)::int as order_close_date,
       coalesce(t.operation_date, NOW() - '1 hours'::INTERVAL * (RANDOM()* 2500)::int -  '1 seconds'::INTERVAL * (RANDOM()* 100)::int) as operation_date
  from tb_logins l
  left join tb_operations t on l.login = t.login and t.operation_type = 'deposit'
  join (select 1 from generate_series(1,5)) gs on 1=1
) t 
where order_close_date >  operation_date 
; commit;

-- The average time of transition between the stages of the funnel

with cte_orders as (
select login, min(order_close_date) as fst_order_close_date
  from tb_orders
  group by login
), cte_operations as (
select login, min(operation_date) as operation_date
  from tb_operations
 where operation_type = 'deposit'
 group by login
)
select u.country,
       avg(op.operation_date::Date - u.registration_date::Date) as reg_oper_date, 
       avg(coalesce(o.fst_order_close_date, op.operation_date)::Date - op.operation_date::Date) as oper_reg_date, 
       count(distinct u.uid) as user_count
  from tb_users u
  join tb_logins l on u.uid = l.user_uid
  join cte_operations op on op.login = l.login
  left join cte_orders o on o.login = l.login
  where l.account_type = 'real' and u.registration_date >= current_date - interval '90 days'
  group by u.country
  order by count(distinct u.uid) desc
;

-- the number of all clients by country with average deposit of >=1000

with cte_avg_deposit as (
select u.uid,
       u.country,
       avg(o.amount) as avg_dep_amount       
  from tb_users u
  join tb_logins l on u.uid = l.user_uid
  join tb_operations o on o.login = l.login and o.operation_type = 'deposit'
 group by u.uid, u.country
) select country,
         count(distinct uid) as user_count,
         sum((avg_dep_amount >= 1000)::int) as user_dep_more_1000_count
    from cte_avg_deposit
    group by country
    order by count(distinct uid) desc
;

-- The first three deposits of each client
select * 
  from (select u.uid,
		       l.login,
		       o.operation_date,
		       row_number() over (partition by uid order by o.operation_date) as dep_number
		  from tb_users u
		  join tb_logins l on u.uid = l.user_uid
		  join tb_operations o on o.login = l.login and o.operation_type = 'deposit'
  ) t
  where dep_number < 4
  order by uid, dep_number
