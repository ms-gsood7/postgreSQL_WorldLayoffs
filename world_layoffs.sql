-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

create table layoffs(
company varchar(20),
	location varchar(20),
	industry varchar(20),
	total_laid_off int,
	percentage_laid_off float,
	layoff_date date,
	stage varchar(20),
	country	varchar(30),
	funds_raised_millions int
)
alter table layoffs
alter column layoff_date type text

alter table layoffs
alter column company type varchar(40)

alter table layoffs
alter column funds_raised_millions type float

select * from layoffs

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

create table layoffs_staging (like layoffs including all)


select * from layoffs_staging

insert into layoffs_staging 
select * from layoffs


--Steps for cleaning:
--1. remove duplicates
--2. Standardize data
--3. null or blank values
--4. remove unnecessary values- Remove Any Columns

---1. remove duplicates

select * from layoffs_staging

with duplicate_cte as
(
select *, row_number() over(
	partition by company,location, industry, total_laid_off, percentage_laid_off,layoff_date,stage,country,funds_raised_millions) as row_num
	from layoffs_staging
)
select * from duplicate_cte
	where row_num > 1
	
--checking for a company to confirm

select * from layoffs_staging
where company ='Casper';

--the result is as expected. We may want to delete the entries where the row number >1

with delet_cte as
(
select *, row_number() over(
	partition by company,location, industry, total_laid_off, percentage_laid_off,layoff_date,stage,country,funds_raised_millions) as row_num
	from layoffs_staging
)
delete 
from delet_cte
where row_num > 1;


drop table layoff_staging_duplicate

--Another solutin is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column

create table layoff_staging_duplicate(
company varchar(40),
	location varchar(20),
	industry varchar(20),
	total_laid_off int,
	percentage_laid_off float,
	layoff_date text,
	stage varchar(20),
	country	varchar(30),
	funds_raised_millions int,
	rownum int
)

select * from layoff_staging_duplicate

insert into layoff_staging_duplicate
select *, row_number() over(
	partition by company,location, industry, total_laid_off, percentage_laid_off,layoff_date,stage,country,funds_raised_millions) as row_num
	from layoffs_staging

select * from layoff_staging_duplicate
where rownum>1

delete from layoff_staging_duplicate
where rownum>1

with duplicate_cte as
(
select *, row_number() over(
	partition by company,location, industry, total_laid_off, percentage_laid_off,layoff_date,stage,country,funds_raised_millions) as row_num
	from layoff_staging_duplicate
)
select * from duplicate_cte
	where row_num > 1


select * from layoff_staging_duplicate
where company ='Casper';

---------------------

--2. standardizing data
--finding issues and fixing it

select company, trim(company)  from layoff_staging_duplicate

update layoff_staging_duplicate
set company = trim(company);


select distinct(industry) from layoff_staging_duplicate
order by 1

-- it looks like Crypto is related to cyrptocurrency, but this one just isn't populated.
-- Crypto has multiple different variations. We need to standardize that.

select * from layoff_staging_duplicate
where industry like '%Crypto%'

update layoff_staging_duplicate
set industry = 'Crypto'
where industry like '%Crypto%'

select distinct(location) from layoff_staging_duplicate
order by 1

select distinct(country) from layoff_staging_duplicate
order by 1


select distinct(country), TRIM(trailing 'n' from country)
from layoff_staging_duplicate
order by 1

--testing and learining from sample queries
select * from TRIM(trailing 'a' from 'baaaaaaaaaa')

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. 
-- Let's standardize this.

update layoff_staging_duplicate
set country = TRIM(trailing '.' from country) 
where country like 'United States%'


select distinct(country) from layoff_staging_duplicate
order by country


--3. formatting date
--string_to_date
select layoff_date from layoff_staging_duplicate


select layoff_staging_duplicate.layoff_date ,TO_DATE(layoff_date, 'MM/DD/YYYY')
from layoff_staging_duplicate

update layoff_staging_duplicate
set layoff_date = TO_DATE(layoff_date, 'MM/DD/YYYY')


select * from layoff_staging_duplicate

alter table layoff_staging_duplicate
alter layoff_date type date using to_date(layoff_date, 'YYYY-MM-DD')


-- 3. Look at Null Values
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. 

select * from layoff_staging_duplicate
where total_laid_off is NULL
and percentage_laid_off is null


--4. remove rows and columns which aren't required

select distinct(industry) from layoff_staging_duplicate


select * from layoffs_staging
where industry is null
or industry = ''


select * from layoff_staging_duplicate
where company = 'Airbnb'

select * from layoff_staging_duplicate
where total_laid_off is NULL

delete from layoff_staging_duplicate
where industry is null

select distinct(industry) from layoff_staging_duplicate
select * from layoff_staging_duplicate
where industry is null


select * from layoff_staging_duplicate st1
join 
layoff_staging_duplicate st2 
on st1.company = st2.company
and st1.location = st2.location
where (st1.industry is null or st1.industry = '')
and (st2.industry is not null or st2.industry !='')

update layoff_staging_duplicate st2
set industry = null where industry =''


update layoff_staging_duplicate st1
set industry = st2.industry
from layoff_staging_duplicate st2 
where 
st1.company = st2.company and
(st1.industry is null or st1.industry = '')
and st2.industry is not null;

select * from layoff_staging_duplicate
where company like '%Bally%'

---no layoffs? no idea about being laid off -- so delete
-- no accuracy

select *
from layoff_staging_duplicate
where total_laid_off is null
and percentage_laid_off is null

delete 
from layoff_staging_duplicate
where total_laid_off is null
and percentage_laid_off is null

-- get rid of rownum

select * from layoff_staging_duplicate

alter table layoff_staging_duplicate
drop column rownum

select distinct(company) from layoff_staging_duplicate

-----Cleaning completed--------

----EDA
-- exploring the data

select * from layoff_staging_duplicate


-- Looking at Percentage to see how big these layoffs were
select max(percentage_laid_off)  , max(total_laid_off) from layoff_staging_duplicate

-- Which companies had 1 which is basically 100 percent of they company laid off
select * from layoff_staging_duplicate
where percentage_laid_off = 1
and funds_raised_millions is not null
order by funds_raised_millions desc
-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt looks like an EV company, Quibi! Raised like 2 billion dollars and went under!!

-- Companies with the most Total Layoffs
select company, sum(total_laid_off)
from layoff_staging_duplicate
group by company
order by 2 desc

select sum(total_laid_off)
from layoff_staging_duplicate

select min(layoff_date), max(layoff_date)
from layoff_staging_duplicate

-- by industry
select industry, sum(total_laid_off)
from layoff_staging_duplicate
group by industry
order by 2 desc

-- by location
select country, sum(total_laid_off)
from layoff_staging_duplicate
group by country
order by 2 desc

-- this it total in the past 3 years or in the dataset


select extract(year from layoff_date) as layoff_year, sum(total_laid_off)
from layoff_staging_duplicate
group by layoff_year
order by 2 desc


select stage, sum(total_laid_off)
from layoff_staging_duplicate
group by stage
order by 2 desc

select substring(layoff_date from 6 for 2) as month
from layoff_staging_duplicate

select EXTRACT(mONTH from LAYOFF_DATE) as layoff_date_month, sum(total_laid_off)
from layoff_staging_duplicate
group by layoff_date_month
order by layoff_date_month


--postgres doesnt allow to extract both year and month in a single query using extract so convert to character using to_char and then extract
select to_char(layoff_date, 'YYYY-MM') as layoff_date_month,
sum(total_laid_off)
from layoff_staging_duplicate
group by layoff_date_month
order by layoff_date_month
;

select * from layoff_staging_duplicate


-- Rolling Total of Layoffs Per Month. 
-- Using it in a CTE so we can query off of it
with rolling_total as
(
select to_char(layoff_date, 'YYYY-MM') as layoff_date_month,
sum(total_laid_off) as total_off 
from layoff_staging_duplicate
group by layoff_date_month
order by layoff_date_month
)
select layoff_date_month,total_off, sum(total_off) over(order by layoff_date_month)
from rolling_total














































