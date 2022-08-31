select*
from PortfolioProject.dbo.covid_deth$
where continent is not null
order by 3,4


--select*
--from PortfolioProject.dbo.covid_vaccinations$
--order by 3,4

-- Select data that we are going to be using

select Location, date, total_cases, new_cases, total_deaths, population
from PortfolioProject.dbo.covid_deth$
where continent is not null
order by 1,2

-- Looking as Total Cases vs Total Deaths
-- Shows liklihood of dying if you contract covid in your country
select Location, date, total_cases, total_deaths, (floor(total_deaths)/floor(total_cases))*100 as DeathPercentage
from PortfolioProject.dbo.covid_deth$
where location like 'Russia' and continent is not null
order by 1,2

-- looking at total cases vs populations
-- Shows whhat precentage of population got Covid

select Location, date, total_cases, population, (floor(total_cases)/population)*100 as PercentPopulationInfected
from PortfolioProject.dbo.covid_deth$
where location like 'Russia' and continent is not null
order by 1,2


-- Looking at Countries with Highest Infection Rate compared to Populations

select Location, population, MAX(total_cases) as HighestInfectionCount, MAX((floor(total_cases)/population))*100 as PercentPopulationInfected
from PortfolioProject.dbo.covid_deth$
-- where location like 'Russia' and continent is not null
group by Location, population
order by PercentPopulationInfected desc


-- Showing Countries with Highest Death count per population

select Location, MAX(floor(total_deaths)) as TotalDeathCount 
from PortfolioProject.dbo.covid_deth$
-- where location like 'Russia'
where continent is not null
group by Location 
order by TotalDeathCount desc


-- Let's break things down by continent 


-- Showing continents with the highest death count per population

select continent, MAX(floor(total_deaths)) as TotalDeathCount 
from PortfolioProject.dbo.covid_deth$
-- where location like 'Russia'
where continent is not null
group by continent 
order by TotalDeathCount desc


-- Global Numbers

select SUM(floor(new_cases))as total_cases, SUM(floor(new_deaths)) as total_death,  SUM(floor(new_deaths))/SUM(floor(new_cases))*100 as DeathPercentage
from PortfolioProject.dbo.covid_deth$
-- where location like 'Russia' 
where continent is not null
--group by date
order by 1,2


-- Looking at total population vs vaccinations

Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(floor(vac.new_vaccinations)) OVER (partition by dea.Location order by dea.Location, dea.date)
as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/floor(population))*100 
from PortfolioProject.dbo.covid_deth$ as dea
join PortfolioProject.dbo.covid_vaccinations$ as vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
order by 2,3


--USE CTE

With PopVsVac (Continent, Location, Date, Population,New_Vaccination, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, floor(dea.population) as population, vac.new_vaccinations
, SUM(convert(int, floor(vac.new_vaccinations))) OVER (partition by dea.Location order by dea.Location, dea.date)
as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/floor(population))*100 
from PortfolioProject.dbo.covid_deth$ as dea
join PortfolioProject.dbo.covid_vaccinations$ as vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
--order by 2,3
)
Select *, (RollingPeopleVaccinated/FLOOR(Population))*100 as RollingPeopleVaccinatedPer
from PopVsVac


-- Temp table

Drop table if exists #PercentPopulationVac
Create table #PercentPopulationVac
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

insert into #PercentPopulationVac
Select dea.continent, dea.location, dea.date, floor(dea.population) as population
, floor(vac.new_vaccinations) as new_vaccinations
, SUM(floor(vac.new_vaccinations)) OVER (partition by dea.Location order by dea.Location, dea.date)
as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/floor(population))*100 
from PortfolioProject.dbo.covid_deth$ as dea
join PortfolioProject.dbo.covid_vaccinations$ as vac
	on dea.location = vac.location
	and dea.date = vac.date
--where dea.continent is not null
--order by 2,3

Select *, (RollingPeopleVaccinated/FLOOR(Population))*100 
from #PercentPopulationVac


-- Creating View to store data for later visualizations

Create View PercentPopulationVac as 
Select dea.continent, dea.location, dea.date, floor(dea.population) as population
, floor(vac.new_vaccinations) as new_vaccinations
, SUM(floor(vac.new_vaccinations)) OVER (partition by dea.Location order by dea.Location, dea.date)
as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/floor(population))*100 
from PortfolioProject.dbo.covid_deth$ as dea
join PortfolioProject.dbo.covid_vaccinations$ as vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
--order by 2,3

select *
from PercentPopulationVac