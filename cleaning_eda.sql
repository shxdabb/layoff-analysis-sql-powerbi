
-- SQL Project - Data Cleaning and EDA

-- Create and use database
CREATE DATABASE project11;
USE project11;

-- Create staging table and insert raw data
CREATE TABLE layoffs_staging LIKE layoffs;
INSERT INTO layoffs_staging SELECT * FROM layoffs;

-- Remove Duplicates
WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging
)
DELETE FROM layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- Create clean staging2 table
CREATE TABLE layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    date TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT,
    row_num INT
);

INSERT INTO layoffs_staging2
SELECT company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions,
       ROW_NUMBER() OVER (
         PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions
       ) AS row_num
FROM layoffs_staging;

-- Remove duplicate rows based on row_num
DELETE FROM layoffs_staging2 WHERE row_num >= 2;

-- Standardize and clean data
UPDATE layoffs_staging2 SET industry = NULL WHERE industry = '';
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;
UPDATE layoffs_staging2 SET industry = 'Crypto' WHERE industry IN ('Crypto Currency', 'CryptoCurrency');
UPDATE layoffs_staging2 SET country = TRIM(TRAILING '.' FROM country);
UPDATE layoffs_staging2 SET date = STR_TO_DATE(date, '%m/%d/%Y');
ALTER TABLE layoffs_staging2 MODIFY COLUMN date DATE;

-- Handle nulls and remove useless rows
DELETE FROM layoffs_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Drop helper column
ALTER TABLE layoffs_staging2 DROP COLUMN row_num;
