create table st.miha_salary_hist (
  person varchar(255), 
  class varchar(255), 
  salary numeric, 
  effective_from date, 
  effective_to date
) with interval_data as (
  select 
    person, 
    class, 
    salary, 
    dt as effective_from, 
    lead(dt) over (
      partition by person 
      order by 
        dt
    ) as next_effective_from 

  from 
    de.histgroup
) insert into st.miha_salary_hist (
  person, class, salary, effective_from, 
  effective_to
) 

select 
  person, 
  class, 
  salary, 
  effective_from, 
  coalesce(
    next_effective_from - interval '1 day', 
    date '2999-12-31'
  ) as effective_to 

from 
  interval_data 
order by 
  person, 
  effective_from;

create table st.miha_salary_log(
  payment_dt timestamp, 
  person varchar(50), 
  payment int, 
  month_paid int, 
  month_rest int
);

insert into st.miha_salary_log (
  payment_dt, person, payment, month_paid, 
  month_rest
) 

select 
  t1.dt as payment_dt, 
  t1.person, 
  t1.payment, 
  sum(t1.payment) over (
    partition by t1.person, 
    date_trunc('month', t1.dt) 
    order by 
      t1.dt rows between unbounded preceding 
      and current row
  ) as month_paid, 
  t2.salary - sum(t1.payment) over (
    partition by t1.person, 
    date_trunc('month', t1.dt) 
    order by 
      t1.dt rows between unbounded preceding 
      and current row
  ) as month_rest 

from 
  de.salary_payments as t1 
  inner join st.miha_salary_hist as t2 on t1.person = t2.person 
  and t1.dt between t2.effective_from 
  and t2.effective_to;