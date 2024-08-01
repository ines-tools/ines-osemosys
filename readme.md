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
- REGION__EMISSION:
  - AnnualEmissionLimit
- REGION__TECHNOLOGY__EMISSION:
  - EmissionActivityRatio

  

