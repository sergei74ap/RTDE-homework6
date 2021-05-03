-- ЗАДАНИЕ: Собрать витрину “Payment report” по модели “Звезда”

-- 0. Подготовим структуру таблиц
-- Вспомним сколько сущностей сохранено в слое DDS DWH
select count(*) from sperfilyev.dds_t_hub_user;
select count(*) from sperfilyev.dds_t_hub_account;
select count(*) from sperfilyev.dds_t_hub_billing_period;
select count(*) from sperfilyev.dds_t_lnk_payment;
select count(*) from sperfilyev.dds_t_sat_user;
select count(*) from sperfilyev.dds_t_sat_user_mdm;
select count(*) from sperfilyev.dds_t_sat_payment;

-- Таблицы измерений
CREATE TABLE sperfilyev.payment_report_dim_billing_year(
    id SERIAL PRIMARY KEY, billing_year_key INT);
CREATE TABLE sperfilyev.payment_report_dim_legal_type(
    id SERIAL PRIMARY KEY, legal_type_key TEXT);
CREATE TABLE sperfilyev.payment_report_dim_district(
    id SERIAL PRIMARY KEY, district_key TEXT);
CREATE TABLE sperfilyev.payment_report_dim_registration_year(
    id SERIAL PRIMARY KEY, registration_year_key INT);

-- Таблица фактов
CREATE TABLE sperfilyev.payment_report_fct(
    billing_year_id INT, legal_type_id INT, district_id INT, registration_year_id INT,
    is_vip BOOLEAN, pay_sum NUMERIC,
    CONSTRAINT fk_billing_year FOREIGN KEY(billing_year_id)
        REFERENCES sperfilyev.payment_report_dim_billing_year(id),
    CONSTRAINT fk_legal_type FOREIGN KEY(legal_type_id)
        REFERENCES sperfilyev.payment_report_dim_legal_type(id),
    CONSTRAINT fk_district FOREIGN KEY(district_id)
        REFERENCES sperfilyev.payment_report_dim_district(id),
    CONSTRAINT fk_registration_year FOREIGN KEY(registration_year_id)
        REFERENCES sperfilyev.payment_report_dim_registration_year(id)
    );

-- 1. Собрать денормализованную таблицу (payment_report_tmp) на основе
-- ссылки payment и двух хабов - user и billing_period, а также таблицы
-- mdm.user, содержащей дополнительные данные по пользователям.
DROP TABLE IF EXISTS sperfilyev.payment_report_tmp_oneyear;
CREATE TABLE sperfilyev.payment_report_tmp_oneyear AS (
  WITH raw_data AS (
      SELECT legal_type,
             district,
             EXTRACT(YEAR FROM su.effective_from) as registration_year,
             is_vip,
             EXTRACT(YEAR FROM to_date(billing_period_key, 'YYYY-MM')) AS billing_year,
             billing_period_key,
             pay_sum
      FROM sperfilyev.dds_t_lnk_payment lp
      JOIN sperfilyev.dds_t_hub_billing_period hbp ON lp.billing_period_pk=hbp.billing_period_pk
      JOIN sperfilyev.dds_t_hub_user hu ON lp.user_pk=hu.user_pk
      JOIN sperfilyev.dds_t_sat_payment sp ON lp.pay_pk=sp.pay_pk
      LEFT JOIN sperfilyev.dds_t_sat_user_mdm su ON hu.user_pk=su.user_pk),
  oneyear_data AS (
      SELECT * FROM raw_data
      WHERE billing_year=2017
  )
SELECT billing_year, legal_type, district, registration_year,
       is_vip, sum(pay_sum)
FROM oneyear_data
GROUP BY billing_year, legal_type, district, registration_year, is_vip
ORDER BY billing_year, legal_type, district, registration_year, is_vip
);

-- Проверка наполнения временной таблицы
select count(*) from sperfilyev.payment_report_tmp_oneyear;
select * from sperfilyev.payment_report_tmp_oneyear limit 100;

-- 2. Создать и наполнить данными из payment_report_tmp таблицы
-- для каждого измерения (payment_report_dim_*).
INSERT INTO sperfilyev.payment_report_dim_billing_year(billing_year_key)
SELECT DISTINCT billing_year AS billing_year_key
FROM sperfilyev.payment_report_tmp_oneyear
LEFT JOIN sperfilyev.payment_report_dim_billing_year ON billing_year_key=billing_year
WHERE billing_year_key is NULL;

INSERT INTO sperfilyev.payment_report_dim_legal_type(legal_type_key)
SELECT DISTINCT legal_type AS legal_type_key
FROM sperfilyev.payment_report_tmp_oneyear
LEFT JOIN sperfilyev.payment_report_dim_legal_type ON legal_type_key=legal_type
WHERE legal_type_key is NULL;

INSERT INTO sperfilyev.payment_report_dim_district(district_key)
SELECT DISTINCT district AS district_key
FROM sperfilyev.payment_report_tmp_oneyear
LEFT JOIN sperfilyev.payment_report_dim_district ON district_key=district
WHERE district_key is NULL;

INSERT INTO sperfilyev.payment_report_dim_registration_year(registration_year_key)
SELECT DISTINCT registration_year AS registration_year_key
FROM sperfilyev.payment_report_tmp_oneyear
LEFT JOIN sperfilyev.payment_report_dim_registration_year ON registration_year_key=registration_year
WHERE registration_year_key is NULL;

-- Проверка наполнения таблиц измерений
select count(*) from sperfilyev.payment_report_dim_billing_year;
select * from sperfilyev.payment_report_dim_billing_year order by id;

select count(*) from sperfilyev.payment_report_dim_legal_type;
select * from sperfilyev.payment_report_dim_legal_type order by id;

select count(*) from sperfilyev.payment_report_dim_district;
select * from sperfilyev.payment_report_dim_district order by id;

select count(*) from sperfilyev.payment_report_dim_registration_year;
select * from sperfilyev.payment_report_dim_registration_year order by id;

-- 3. Создать и наполнить данными из payment_report_tmp таблицу 
-- для фактов (payment_report_fct).
INSERT INTO sperfilyev.payment_report_fct
SELECT biy.id, lt.id, d.id, ry.id, tmp.is_vip, tmp.sum
FROM sperfilyev.payment_report_tmp_oneyear tmp
JOIN sperfilyev.payment_report_dim_billing_year biy ON tmp.billing_year=biy.billing_year_key
JOIN sperfilyev.payment_report_dim_legal_type lt ON tmp.legal_type=lt.legal_type_key
JOIN sperfilyev.payment_report_dim_district d ON tmp.district=d.district_key
JOIN sperfilyev.payment_report_dim_registration_year ry ON tmp.registration_year=ry.registration_year_key;

-- Проверка наполнения таблицы фактов
select count(*) from sperfilyev.payment_report_fct;
select * from sperfilyev.payment_report_fct;
-- Сравним общую сумму с первоисточником в ODS:
select sum(pay_sum) from sperfilyev.payment_report_fct;
-- сумма = 238,171,289 (в случае формирования витрины по годам в Airflow)
select count(*) from sperfilyev.ods_t_payment;
select sum(pay_sum) from sperfilyev.ods_t_payment;
-- сумма = 248,435,900

-- ======================================================
-- Очистить временную таблицу
truncate sperfilyev.payment_report_tmp_oneyear;
-- Очистить витрину
truncate sperfilyev.payment_report_fct;
-- Очистить измерения
truncate sperfilyev.payment_report_dim_billing_year;
truncate sperfilyev.payment_report_dim_legal_type;
truncate sperfilyev.payment_report_dim_district;
truncate sperfilyev.payment_report_dim_registration_year;
