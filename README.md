# 2023-portfolio-covid
--Just a draft, Need to add more description
--This work is done on BigQuery


SELECT *
FROM `myportfolio-covid19.coviddeaths.coviddeaths`
WHERE continent is not null

SELECT *
FROM `myportfolio-covid19.coviddata.coviddeaths`
ORDER BY 3,4

SELECT location, date, total_cases, total_deaths, population
FROM `myportfolio-covid19.coviddata.coviddeaths`
ORDER BY 1,2

SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)
FROM `myportfolio-covid19.coviddata.coviddeaths`
ORDER BY 1,2

SELECT location, date, total_cases,population,(total_cases/population)*100 AS PopPercentage
FROM `myportfolio-covid19.coviddata.coviddeaths`
WHERE location LIKE '%Thailand%'
ORDER BY 1,2

SELECT location,population,MAX(total_cases) AS HighestInfectionCount, MAX((total_cases/population))*100 AS PopulatInfectPercent
FROM `myportfolio-covid19.coviddata.coviddeaths`
--WHERE location LIKE '%Thailand%'
GROUP BY location, population
ORDER BY 1,2

SELECT location,population,MAX(total_cases) AS HighestInfectionCount, MAX((total_cases/population))*100 AS PopulatInfectPercent
FROM `myportfolio-covid19.coviddata.coviddeaths`
--WHERE location LIKE '%Thailand%'
GROUP BY location, population
ORDER BY PopulatInfectPercent DESC

SELECT location,MAX((total_deaths)) AS TotalDeathCount
FROM `myportfolio-covid19.coviddata.coviddeaths`
--WHERE location LIKE '%Thailand%'
WHERE continent is not null
GROUP BY location
ORDER BY TotalDeathCount DESC

SELECT continent,MAX((total_deaths)) AS TotalDeathCount
FROM `myportfolio-covid19.coviddata.coviddeaths`
--WHERE location LIKE '%Thailand%'
WHERE continent is not null
GROUP BY continent
ORDER BY TotalDeathCount DESC

SELECT date, SUM(new_cases), SUM(new_deaths), -- (total_deaths/total_cases)*100 AS DeathPercentage
FROM `myportfolio-covid19.coviddata.coviddeaths`
--WHERE location LIKE '%Thailand%'
WHERE continent is not null
GROUP BY date
ORDER BY 1,2

SELECT SUM(new_cases) AS Total_cases, SUM(new_deaths) AS Total_deaths, --SUM(new_deaths)/SUM(new_cases)*100 AS DeathPercentage
FROM `myportfolio-covid19.coviddata.coviddeaths`
--WHERE location LIKE '%Thailand%'
WHERE continent is not null
GROUP BY date
ORDER BY 1,2

SELECT date, SUM(new_cases) AS Total_cases, SUM(new_deaths) AS Total_deaths, --SUM(new_deaths)/SUM(new_cases)*100 AS DeathPercentage
FROM `myportfolio-covid19.coviddata.coviddeaths`
--WHERE location LIKE '%Thailand%'
WHERE continent is not null
GROUP BY date
ORDER BY 1,2

SELECT SUM(new_cases) AS Total_cases, SUM(new_deaths) AS Total_deaths,SUM(new_deaths)/SUM(new_cases)*100 AS DeathPercentage
FROM `myportfolio-covid19.coviddata.coviddeaths`
--WHERE location LIKE '%Thailand%'
WHERE continent is not null
--GROUP BY date
ORDER BY 1,2

SELECT DEATH.continent, DEATH.location,DEATH.date, DEATH.population, VACC.new_vaccinations
FROM `myportfolio-covid19.coviddata.coviddeaths` AS DEATH
JOIN `myportfolio-covid19.coviddata.covidvaccination` AS VACC
ON DEATH.location = VACC.location
AND DEATH.date = VACC.date
WHERE DEATH.continent is not null

SELECT DEATH.continent, DEATH.location,DEATH.date, DEATH.population, VACC.new_vaccinations,
  SUM(VACC.new_vaccinations) OVER (PARTITION BY DEATH.location ORDER BY DEATH.location, DEATH.date) 
  AS PeopleVaccinated
FROM `myportfolio-covid19.coviddata.coviddeaths` AS DEATH
JOIN `myportfolio-covid19.coviddata.covidvaccination` AS VACC
ON DEATH.location = VACC.location
AND DEATH.date = VACC.date
WHERE DEATH.continent is not null

WITH PopulatvsVacc AS (
  SELECT 
    DEATH.continent, 
    DEATH.location,
    DEATH.date, 
    DEATH.population, 
    VACC.new_vaccinations,
    SUM(VACC.new_vaccinations) OVER (
      PARTITION BY DEATH.location 
      ORDER BY DEATH.location, DEATH.date
    ) AS PeopleVaccinated
  FROM `myportfolio-covid19.coviddata.coviddeaths` AS DEATH
  JOIN `myportfolio-covid19.coviddata.covidvaccination` AS VACC
    ON DEATH.location = VACC.location
    AND DEATH.date = VACC.date
  WHERE DEATH.continent is not null
)

SELECT *, (PeopleVaccinated/population)*100
FROM PopulatvsVacc;


CREATE TABLE IF NOT EXISTS `myportfolio-covid19.coviddata.PercentPopulationVaccinated`
(
continent string,
location string,
date datetime,
population numeric,
new_vaccination numeric,
PeopleVaccinated numeric
);

INSERT INTO `myportfolio-covid19.coviddata.PercentPopulationVaccinated`
SELECT DEATH.continent, DEATH.location,DEATH.date, DEATH.population, VACC.new_vaccinations,
  SUM(VACC.new_vaccinations) OVER (PARTITION BY DEATH.location ORDER BY DEATH.location, DEATH.date) 
  AS PeopleVaccinated
FROM `myportfolio-covid19.coviddata.coviddeaths` AS DEATH
JOIN `myportfolio-covid19.coviddata.covidvaccination` AS VACC
ON DEATH.location = VACC.location
AND DEATH.date = VACC.date;

SELECT *, (PeopleVaccinated/population)*100
FROM `myportfolio-covid19.coviddata.PercentPopulationVaccinated`;

--Create view

CREATE VIEW `myportfolio-covid19.coviddata.myPercentPopulationVaccinated`
AS
SELECT 
    DEATH.continent, 
    DEATH.location,
    DEATH.date, 
    DEATH.population, 
    VACC.new_vaccinations,
    SUM(VACC.new_vaccinations) OVER (
      PARTITION BY DEATH.location 
      ORDER BY DEATH.location, DEATH.date
    ) AS PeopleVaccinated
  FROM `myportfolio-covid19.coviddata.coviddeaths` AS DEATH
  JOIN `myportfolio-covid19.coviddata.covidvaccination` AS VACC
    ON DEATH.location = VACC.location
    AND DEATH.date = VACC.date
  WHERE DEATH.continent is not null ;

  --Export file
  EXPORT DATA
  OPTIONS(
    uri='gs://my-bucket/my-file.csv',
    format='CSV',
    overwrite=true,
    header=true
  ) AS
SELECT * FROM `myportfolio-covid19.coviddata.PercentPopulationVaccinated`;


