## OSeMOSYS to ines conversion

This package converts an OSeMOSYS (version 2017_11_08) Spine Toolbox database (that has been imported to a Spine DB using MathProg import/read scripts from the ines-tools repository) to an ines-spec compatible Spine Toolbox database.

## Missing conversions from OSeMOSYS data to ines-spec (these are ignored at the moment)

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
  - CapacityOfOneTechnologyUnit (This is meant for MIP problems, instead capacity of 1000 MW is assumed for all units since OSeMOSYS does not sepately define unit size)
  - CapacityToActivityUnit (Converts capacity to annual energy, unclear how it really works)
  - ReserveMarginTagTechnology
  - TotalAnnualMaxCapacityInvestment
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
  - TechnologyFromStorage  (unnecessary flags, since these are represented by directionality in ines)
  - TechnologyToStorage  (unnecessary flags, since these are represented by directionality in ines)
- REGION__EMISSION:
  - AnnualEmissionLimit
- REGION__TECHNOLOGY__EMISSION:
  - EmissionActivityRatio
- REGION__REGION__FUEL:
  - TradeRoute
  

