--
-- The average time of transition between the stages of the funnel
--
with cte_orders as (
select login_hash_key, min(order_close_date) as fst_order_close_date
  from vault.s_orders
  group by login_hash_key
), cte_operations as (
select login_hash_key, min(operation_date) as fst_operation_date
  from vault.s_operations
 where operation_type = 'deposit'
 group by login_hash_key
)
select u.country,
       avg(op.fst_operation_date::Date - u.registration_date::Date) as reg_oper_date, 
       avg(coalesce(o.fst_order_close_date, op.fst_operation_date)::Date - op.fst_operation_date::date) as oper_reg_date,
       count(distinct u.user_hash_key) as user_count
  from vault.s_users u
  join vault.l_user_login l on u.user_hash_key = l.user_hash_key
  join vault.s_logins sl on l.login_hash_key = sl.login_hash_key and sl.account_type = 'real'
  join cte_operations op on op.login_hash_key = l.login_hash_key 
  left join cte_orders o on o.login_hash_key = l.login_hash_key
  where u.registration_date >= current_date - interval '90 days'
  group by u.country
  order by count(distinct u.user_hash_key) desc
;

-- the number of all clients by country with average deposit of >=1000

with cte_avg_deposit as (
select u.user_hash_key,
       u.country,
       avg(o.amount) as avg_dep_amount       
  from vault.s_users u
  join vault.l_user_login l on u.user_hash_key = l.user_hash_key
  join vault.s_operations o on o.login_hash_key = l.login_hash_key and o.operation_type = 'deposit'
 group by u.user_hash_key, u.country
) select country,
         count(distinct user_hash_key) as user_count,
         sum((avg_dep_amount >= 1000)::int) as user_dep_more_1000_count
    from cte_avg_deposit
    group by country
    order by count(distinct user_hash_key) desc
;

-- The first three deposits of each client
select * 
  from (select u.user_hash_key,
		       o.login_hash_key,
		       o.operation_date,
		       row_number() over (partition by u.user_hash_key order by o.operation_date) as dep_number
		  from vault.l_user_login u
		  join vault.s_operations o on o.login_hash_key = u.login_hash_key and o.operation_type = 'deposit'
  ) t
  where dep_number < 4
  order by user_hash_key, dep_number