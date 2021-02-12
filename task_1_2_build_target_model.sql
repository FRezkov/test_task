-- Create a database structure

create schema if not exists vault;

/*1.Creating a hub table for users. 
 *  Surrogate keys user_hash_key is hash from uid.*/
drop table if exists vault.h_users;
create table vault.h_users (
	user_hash_key uuid NOT NULL,
	uid uuid NOT NULL,
	load_dttm timestamp without time zone,
	source varchar(128),
	CONSTRAINT h_users_pk PRIMARY KEY(user_hash_key)
);commit;

/*2. Creating a hub table for logins. 
 *   Surrogate key login_hash_key is hash from login.*/
drop table if exists vault.h_logins;
create table vault.h_logins (
	login_hash_key uuid NOT NULL,
	login varchar(36) NOT NULL,
	load_dttm timestamp without time zone,
	source varchar(128),
	CONSTRAINT h_logins_pk PRIMARY KEY(login_hash_key)
);commit;

/*3. Creating a link table between users and orders. 
 *   Surrogate key lnk_hash_key is hash from user_hash_key and login_hash_key.*/
drop table if exists vault.l_user_login;
create table vault.l_user_login (
	l_hash_key uuid NOT NULL,
	user_hash_key uuid,
	login_hash_key uuid,
	load_dttm timestamp without time zone,
	source varchar(128),
	CONSTRAINT l_user_login_pk PRIMARY KEY(l_hash_key)
);commit;

/*4. Creating a satellite table for operations.*/
drop table if exists vault.s_operations;
create table vault.s_operations (
    login_hash_key uuid NOT NULL,
	operation_type text NOT NULL,
	operation_date timestamp NOT NULL,
	amount numeric,
	source varchar(128),
	load_dttm timestamp without time zone,
	update_dttm timestamp,
	CONSTRAINT s_operations_pk PRIMARY KEY(login_hash_key, operation_type, operation_date)
);commit;

/*5. Creating a satellite table for orders.*/
drop table if exists vault.s_orders;
create table vault.s_orders (
    login_hash_key uuid NOT NULL,
	order_close_date timestamp NOT NULL,
	source varchar(128),
	load_dttm timestamp without time zone,
	update_dttm timestamp,
	CONSTRAINT s_orders_pk PRIMARY KEY(login_hash_key, order_close_date)
);commit;

/*6. Creating a satellite table for users.*/
drop table if exists vault.s_users;
create table vault.s_users (
    user_hash_key uuid NOT NULL,
	registration_date timestamp NOT NULL,
	country varchar(128),
	source varchar(128),
	load_dttm timestamp without time zone,
	update_dttm timestamp,
	CONSTRAINT s_users_pk PRIMARY KEY(user_hash_key)
);commit;

/*7. Creating a satellite table for logins.*/
drop table if exists vault.s_logins;
create table vault.s_logins (
    login_hash_key uuid NOT NULL,
	account_type varchar(8),
	source varchar(128),
	load_dttm timestamp without time zone,
	update_dttm timestamp,
	CONSTRAINT s_logins_pk PRIMARY KEY(login_hash_key)
);commit;

/*8. FIll target table with source data.*/

--Believe that the data does not require verification for duplicates

-- FIll h_users hub table
insert into vault.h_users (user_hash_key, uid, load_dttm, "source")
select uuid_in(md5(uid::text)::cstring) as user_hash_key,
       uid, 
       current_timestamp as load_dttm,
       'raw_data.tb_users' as "source"
  from raw_data.tb_users
;
-- FIll s_users satelit table
insert into vault.s_users (user_hash_key, registration_date, country, "source", load_dttm, update_dttm)
select hu.user_hash_key,
       tu.registration_date, 
       tu.country,
       'raw_data.tb_users' as "source",
       current_timestamp as load_dttm,
       current_timestamp as update_dttm
  from vault.h_users hu
  join raw_data.tb_users tu using (uid)
;

-- FIll h_logins hub table
insert into vault.h_logins (login_hash_key, login, load_dttm, "source")
select uuid_in(md5(login::text)::cstring) as login_hash_key,
       login,
       current_timestamp as load_dttm,
       'raw_data.tb_logins' as "source"
  from raw_data.tb_logins
;

-- FIll l_user_login link table
insert into vault.l_user_login (l_hash_key, user_hash_key, login_hash_key, load_dttm, "source")
select uuid_in(md5(l.user_uid::text||l.login)::cstring) l_hash_key,
       hu.user_hash_key,
       hl.login_hash_key,
       current_timestamp as load_dttm,
       'raw_data.tb_logins' as "source"
  from raw_data.tb_logins l 
  join vault.h_users hu on l.user_uid = hu.uid 
  join vault.h_logins hl on l.login = hl.login
;

-- FIll s_operations satelite table
insert into vault.s_operations (login_hash_key, operation_type, operation_date, amount, "source", load_dttm, update_dttm)
select hl.login_hash_key,
       t.operation_type, 
       t.operation_date,
       t.amount,
       'raw_data.tb_operations' as "source",
       current_timestamp as load_dttm,
       current_timestamp as update_dttm
  from vault.h_logins hl
  join raw_data.tb_operations t using (login)
;

-- FIll s_orders satelite table
insert into vault.s_orders (login_hash_key, order_close_date, "source", load_dttm, update_dttm)
select hl.login_hash_key,
       t.order_close_date, 
       'raw_data.tb_orders' as "source",
       current_timestamp as load_dttm,
       current_timestamp as update_dttm
  from vault.h_logins hl
  join raw_data.tb_orders t using (login)
;

-- FIll s_logins satelit table
insert into vault.s_logins (login_hash_key, account_type, "source", load_dttm, update_dttm)
select hl.login_hash_key,
       tl.account_type,
       'raw_data.tb_logins' as "source",
       current_timestamp as load_dttm,
       current_timestamp as update_dttm
  from vault.h_logins hl
  join raw_data.tb_logins tl using (login)
;






