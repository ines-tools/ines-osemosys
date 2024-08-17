# Missing conversions from OSeMOSYS data to ines-spec

- REGION
  - REMinProductionTarget
  - ReserveMargin
  - DepreciationMethod
- REGION__FUEL
  - RETagFuel
  - ReserveMarginTagFuel
- REGION__TECHNOLOGY
  - AvailabilityFactor  (In OSeMOSYS this is annual value, while CapacityFactor is also for timeslices - we only take CapacityFactor at the moment)
  - RETagTechnology
  - CapacityOfOneTechnologyUnit (This is meant for MIP problems, capacity of 1000 MW is assumed for all units
  - CapacityToActivityUnit (Converts capacity to annual energy, unclear how it really works)
  - ReserveMarginTagTechnology
  - TotalAnnualMaxCapacity
  - TotalAnnualMaxCapacityInvestment
  - TotalAnnualMinCapacity
  - TotalAnnualMinCapacityInvestment
  - TotalTechnologyAnnualActivityLowerLimit
  - TotalTechnologyAnnualActivityUpperLimit
  - TotalTechnologyModelPeriodActivityLowerLimit
  - TotalTechnologyModelPeriodActivityUpperLimit
- REGION_STORAGE
  - MinStorageCharge
  - StorageLevelStart
  - StorageMaxChargeRate
- REGION__TECHNOLOGY__STORAGE:
  - TechnologyFromStorage  (unnecessary flags)
  - TechnologyToStorage  (unnecessary flags)
- REGION__EMISSION:
  - AnnualEmissionLimit
- REGION__TECHNOLOGY__EMISSION:
  - EmissionActivityRatio

  

