## OSeMOSYS to ines conversion

This package converts an OSeMOSYS (version 2017_11_08) Spine Toolbox database (that has been imported to a Spine DB using MathProg import/read scripts from the ines-tools repository) to an ines-spec compatible Spine Toolbox database. From ines-spec, the OSeMOSYS model can then be converted to other modelling tools and used in model couplings.

### How to use

This is an outline - content needs to be added

- [Install Spine Toolbox from sources](https://github.com/spine-tools/Spine-Toolbox?tab=readme-ov-file#installation-from-sources-using-git). This is to have an environment where both Spine Toolbox and ines-tools work. (pip install might also work, but this is safer)
- Clone [ines-tools](https://github.com/energy-modelling-workbench/ines-tools) repository
- Clone this ines-osemosys repository parallel to ines-tools (e.g. both under "data_conversions" folder)
- If no yaml package in Python environment/path, add it through `pip install pyyaml` 
- Run Spine Toolbox
- Add source files to a 'Data connection' (model.mod, model.dat, and timeslices_to_time from examples folder (or your own data), settings_OSeMOSYS.yaml from ines-tools/ines_tools/tool_specific/mathprog)
- Add the tools using 'Specifications' --> 'New specification' --> 'From specification file'
  - All OSeMOSYS specifications are in folder: "ines-osemosys/.spinetoolbox/specifications/"
- Drag the newly added specifications to the workflow (thereby creating a tool instance).
  - Modify the tool specifications (right click on the tool icon) to a Python environment with Spine Toolbox and ines-tools.
- Create a Spine database for OSeMOSYS data
- Create a Spine database for ines-spec data
- Connect the tools
- Run the workflow

## From ines to OSeMOSYS conversion

Not implemented as of yet.

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
  

