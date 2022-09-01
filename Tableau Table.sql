
--Queries used for Tableau Project


-- 1. Total cases in the world and Russia

(select SUM(floor(new_cases)) as total_cases
	, SUM(floor(new_deaths)) as total_deaths
	, SUM(floor(new_deaths))/SUM(floor(New_Cases))*100 as DeathPercentage
from PortfolioProject.dbo.covid_deth$
where continent is not null 
)
union
(select SUM(floor(new_cases)) 
	, SUM(floor(new_deaths))
	, SUM(floor(new_deaths))/SUM(floor(New_Cases))*100
from PortfolioProject.dbo.covid_deth$
where continent is not null and location like 'Russia'
)



-- 2. Number of deaths in different parts of the world

select location
	, SUM(floor(new_deaths)) as TotalDeathCount
from PortfolioProject.dbo.covid_deth$
where continent is null 
and location not in ('World', 'European Union', 'International', 'Upper middle income', 'Lower middle income', 'Low income', 'High income')
group by location
order by TotalDeathCount desc



-- 3. Percent Population Infected in different countries

select Location
	, Population
	, MAX(floor(total_cases)) as HighestInfectionCount
	, MAX((floor(total_cases)/floor(population)))*100 as PercentPopulationInfected
from PortfolioProject.dbo.covid_deth$
group by Location, Population
order by PercentPopulationInfected desc



-- 4.  Percent Population Infected Date

select Location
	, Population
	, date
	, MAX(floor(total_cases)) as HighestInfectionCount
	, Max((floor(total_cases)/floor(population)))*100 as PercentPopulationInfected
from PortfolioProject.dbo.covid_deth$
group by Location, Population, date
order by PercentPopulationInfected desc



-- 5. People Vaccinated in different countries

select dea.location
	, dea.population
	, MAX(vac.total_vaccinations) as RollingPeopleVaccinated
from PortfolioProject.dbo.covid_deth$ dea
join PortfolioProject.dbo.covid_vaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
group by dea.location, dea.population
order by 1,2,3



-- 6. Date and total cases in Russia 

Select Location
	, date
	, population
	, total_cases
	, total_deaths
From PortfolioProject.dbo.covid_deth$
where continent is not null and location like 'Russia'
order by 1,2



-- 7. Percent People Vaccinated Date

With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent
	, dea.location
	, dea.date
	, floor(dea.population)
	, floor(vac.new_vaccinations)
	, SUM(floor(vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) 
From PortfolioProject.dbo.covid_deth$ dea
Join PortfolioProject.dbo.covid_vaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
)
Select *, (RollingPeopleVaccinated/floor(Population))*100 as PercentPeopleVaccinated
From PopvsVac
