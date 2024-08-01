import spinedb_api as api
from spinedb_api import DatabaseMapping
from ines_tools import ines_transform
import sys
import yaml

if len(sys.argv) > 1:
    url_db_in = sys.argv[1]
else:
    url_db_in = "sqlite:///C:/data/Toolbox_projects/OSeMOSYS_FlexTool/OSeMOSYS_db.sqlite"
if len(sys.argv) > 2:
    url_db_out = sys.argv[2]
else:
    url_db_out = "sqlite:///C:/data/Spine/ines-osemosys/ines-osemosys/ines-spec.sqlite"

with open('osemosys_to_ines_entities.yaml', 'r') as file:
    entities_to_copy = yaml.load(file, yaml.BaseLoader)
with open('osemosys_to_ines_parameters.yaml', 'r') as file:
    parameter_transforms = yaml.load(file, yaml.BaseLoader)
with open('osemosys_to_ines_methods.yaml', 'r') as file:
    parameter_methods = yaml.load(file, yaml.BaseLoader)
with open('osemosys_to_ines_entities_to_parameters.yaml', 'r') as file:
    entities_to_parameters = yaml.load(file, yaml.BaseLoader)


def main():
    with DatabaseMapping(url_db_in) as source_db:
        with DatabaseMapping(url_db_out) as target_db:
            ## Empty the database
            target_db.purge_items('parameter_value')
            target_db.purge_items('entity')
            target_db.purge_items('alternative')
            #target_db.refresh_session()
            target_db.commit_session("Purged stuff")
            ## Copy alternatives
            for alternative in source_db.get_alternative_items():
                target_db.add_alternative_item(name=alternative.get('name'))
            try:
                target_db.commit_session("Added alternatives")
            except:
                exit("no alternatives in the source database, check the URL for the DB")

            ## Copy entites
            target_db = ines_transform.copy_entities(source_db, target_db, entities_to_copy)
            ## Copy timeslice parameters (manual scripting)
            target_db = process_timeslice_data(source_db, target_db)
            ## Copy numeric parameters(source_db, target_db, parameter_transforms)
            target_db = ines_transform.transform_parameters_use_default(source_db, target_db, parameter_transforms,
                                                                        default_alternative="base", ts_to_map=True)
            ## Copy method parameters
            #target_db = ines_transform.process_methods(source_db, target_db, parameter_methods)
            ## Copy entities to parameters
            #target_db = ines_transform.copy_entities_to_parameters(source_db, target_db, entities_to_parameters)
            ## Copy capacity specific parameters (manual scripting)
            target_db = process_capacities(source_db, target_db)
            ## Create a timeline from start_time and duration
            #target_db = create_timeline(source_db, target_db)
            try:
                target_db.commit_session("Added parameter values")
            except:
                print("commit parameters error")


def process_timeslice_data(source_db, target_db):
    model_items = source_db.get_entity_items(entity_class_name="model")
    if len(model_items) > 1:
        exit("OSeMOSYS to ines script does not handle databases with more than one model entity")
    if len(model_items) == 0:
        exit("No model entities and associated parameters")
    model_item = model_items[0]
    timeslices_to_time = source_db.get_parameter_value_items(entity_class_name="model",
                                                             entity_name=model_item["name"],
                                                             parameter_definition_name="timeslices_to_time")
    if not timeslices_to_time:
        exit("No timeslices to datetime mapping")
    if len(timeslices_to_time) > 1:
        exit("More timeslices_to_time entities than 1 - don't know which one to use, please delete all but 1")
    timeslice_to_time_data = api.from_database(timeslices_to_time[0]["value"], timeslices_to_time[0]["type"])
    datetime_indexes = timeslice_to_time_data.indexes
    timeslice_indexes = []
    #time_duration = []
    previous_time_duration = timeslice_to_time_data.values[0].values[0]
    for time_object in timeslice_to_time_data.values:
        timeslice_indexes.append(time_object.indexes[0])
        #time_duration.append(time_object.values[0])
        if previous_time_duration != time_object.values[0]:
            exit("Variable time resolution not suppported, please make a timeslice to datetime mapping with only one time resolution (use the lowest common denominator)")
    # Store the model time resolution in ines_db
    added, error = target_db.add_entity_item(entity_class_name="temporality",
                                             name=model_item["name"])
    if error:
        exit("Could not add temporality entity to ines-db: " + error)
    p_value, p_type = api.to_database(previous_time_duration)
    added, error = target_db.add_parameter_value_item(entity_class_name="temporality",
                                                      parameter_definition_name="resolution",
                                                      entity_byname=tuple([model_item["name"]]),
                                                      alternative_name=timeslices_to_time[0]["alternative_name"],
                                                      value=p_value,
                                                      type=p_type)
    if error:
        exit("Could not add resolution parameter to ines-db: " + error)

    for year_split in source_db.get_parameter_value_items(entity_class_name="model",
                                                          entity_name=model_item["name"],
                                                          parameter_definition_name="YearSplit"):
        year_split_data = api.from_database(year_split["value"], year_split["type"])
        target_db = add_timeslice_data(source_db, target_db, year_split_data, year_split["alternative_name"],
                                       "REGION__FUEL", "SpecifiedDemandProfile", "node", "flow_profile",
                                       timeslice_indexes, datetime_indexes)
        target_db = add_timeslice_data(source_db, target_db, year_split_data, year_split["alternative_name"],
                                       "REGION__TECHNOLOGY", "CapacityFactor", "unit", "availability",
                                       timeslice_indexes, datetime_indexes)
    return target_db

def add_timeslice_data(source_db, target_db, year_split_data, alternative_name, source_class_name,
                       source_param_name, target_class_name, target_param_name, timeslice_indexes, datetime_indexes):
    for source_class in source_db.get_entity_items(entity_class_name=source_class_name):
        for source_params in source_db.get_parameter_value_items(entity_class_name=source_class_name,
                                                                 entity_name=source_class["name"],
                                                                 parameter_definition_name=source_param_name,
                                                                 alternative_name=alternative_name):
            profile_data = source_params["parsed_value"]  # api.from_database(demand_profile["value"], demand_profile["type"])
            timeslice_profiles = {}
            # profile_timeslice_indexes = []
            for s, profile_data_by_slices in enumerate(profile_data.values):
                # for y, profile_data_by_slices_by_year in enumerate(profile_data_by_slices.values):
                timeslice_profiles[profile_data.indexes[s]] = round(float(profile_data_by_slices.values[0])  # Note that this takes the first value from the array of years (first year)
                                                                    / float(year_split_data.values[s].values[0]), 6)
                # profile_timeslice_indexes.append(profile_data.indexes[s])
            datetime_profiles = []
            for t, timeslice_index in enumerate(timeslice_indexes):
                datetime_profiles.append(timeslice_profiles[timeslice_index])
            to_db_profile_data = api.TimeSeriesVariableResolution(
                datetime_indexes,
                datetime_profiles,
                ignore_year=False,
                repeat=False
            )
            profile_data_divided, p_type = api.to_database(to_db_profile_data)
            added, error = target_db.add_parameter_value_item(entity_class_name=target_class_name,
                                                              parameter_definition_name=target_param_name,
                                                              entity_byname=tuple(
                                                                  ['__'.join(source_params["entity_byname"])]),
                                                              alternative_name=alternative_name,
                                                              value=profile_data_divided,
                                                              type=p_type)
            if error:
                print("process timeslice data error: " + error)
    try:
        target_db.commit_session("Added parameter values for timeslice data")
    except:
        print("failed to add parameter values for timeslice data")
    return target_db

def process_capacities(source_db, target_db):
    region__tech__fuel_entities = source_db.get_entity_items(entity_class_name="REGION__TECHNOLOGY__FUEL")
    for unit_source in source_db.get_entity_items(entity_class_name="REGION__TECHNOLOGY"):
        source_unit_existing_units = {}
        source_unit_investment_cost = {}
        source_unit_fixed_cost = {}
        source_unit_variable_cost = {}

        # Store parameter existing_units (for the alternatives that define it)
        params = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="ResidualCapacity")
        source_unit_existing_units.update(params_to_dict(params))
        params = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="CapitalCost")
        source_unit_investment_cost.update(params_to_dict(params))
        params = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="FixedCost")
        source_unit_fixed_cost.update(params_to_dict(params))
        params = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="VariableCost")
        source_unit_variable_cost.update(params_to_dict(params))

        input_act_ratio = []
        output_act_ratio = []
        for rtf_ent in region__tech__fuel_entities:
            if rtf_ent["entity_byname"][0] + rtf_ent["entity_byname"][1] == unit_source["entity_byname"][0] + unit_source["entity_byname"][1]:
                params = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__FUEL", entity_name=rtf_ent["name"], parameter_definition_name="InputActivityRatio")
                if params:
                    input_act_ratio.append(params_to_dict(params))
                params = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__FUEL", entity_name=rtf_ent["name"], parameter_definition_name="OutputActivityRatio")
                if params:
                    output_act_ratio.append(params_to_dict(params))
                print(unit_source["name"], rtf_ent["name"])

        if len(input_act_ratio) == 1 and len(output_act_ratio) == 1:
            conversion_method = "constant_efficiency"
            for alt_i, iar in input_act_ratio[0]:
                for alt_o, oar in output_act_ratio[0]:
                    efficiency = oar / iar


        # Write 'existing' capacity and virtual_unitsize to FlexTool DB (if capacity defined in unit outputs)
        if u_to_n_capacity:
            for u_to_n_alt, u_to_n_val in u_to_n_capacity.items():
                alt_ent_class = (u_to_n_alt, unit_source["entity_byname"], "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "existing", alt_ent_class, u_to_n_val)
            if existing_units:
                for u_to_n_alt, u_to_n_val in u_to_n_capacity.items():
                    for existing_alt, existing_val in existing_units.items():
                        alt_ent_class = (u_to_n_alt, unit_source["entity_byname"], "unit")
                        virtual_unit_size = u_to_n_val / existing_val
                        target_db = ines_transform.add_item_to_DB(target_db, "virtual_unitsize", alt_ent_class, virtual_unit_size)
                        if u_to_n_alt is not existing_alt:
                            alt_ent_class = (existing_alt, unit_source["entity_byname"], "unit")
                            target_db = ines_transform.add_item_to_DB(target_db, "virtual_unitsize", alt_ent_class, virtual_unit_size)
        # Write 'existing' capacity and virtual_unitsize to FlexTool DB (if capacity is defined in unit inputs instead)
        elif n_to_u_capacity:
            for n_to_u_alt, n_to_u_val in n_to_u_capacity.items():
                alt_ent_class = (n_to_u_alt, unit_source["entity_byname"], "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "existing", alt_ent_class, n_to_u_val)
            if existing_units:
                for n_to_u_alt, n_to_u_val in n_to_u_capacity.items():
                    for existing_alt, existing_val in existing_units.items():
                        alt_ent_class = (n_to_u_alt, unit_source["entity_byname"], "unit")
                        virtual_unit_size = n_to_u_val / existing_val
                        target_db = ines_transform.add_item_to_DB(target_db, "virtual_unitsize", alt_ent_class, virtual_unit_size)
                        if n_to_u_alt is not existing_alt:
                            alt_ent_class = (existing_alt, unit_source["entity_byname"], "unit")
                            target_db = ines_transform.add_item_to_DB(target_db, "virtual_unitsize", alt_ent_class, virtual_unit_size)

        # Write 'investment_cost', 'fixed_cost' and 'salvage_value' to FlexTool DB (if investment_cost defined in unit outputs)
        if u_to_n_investment_cost:
            for u_to_n_alt, u_to_n_val in u_to_n_investment_cost.items():
                alt_ent_class = (u_to_n_alt, unit_source["entity_byname"], "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "invest_cost", alt_ent_class, u_to_n_val)
            for u_to_n_alt, u_to_n_val in u_to_n_fixed_cost.items():
                alt_ent_class = (u_to_n_alt, unit_source["entity_byname"], "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "fixed_cost", alt_ent_class, u_to_n_val)
            for u_to_n_alt, u_to_n_val in u_to_n_salvage_value.items():
                alt_ent_class = (u_to_n_alt, unit_source["entity_byname"], "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "salvage_value", alt_ent_class, u_to_n_val)
        # Write 'investment_cost', 'fixed_cost' and 'salvage_value' to FlexTool DB (if investment_cost is defined in unit inputs instead)
        elif n_to_u_investment_cost:
            for n_to_u_alt, n_to_u_val in n_to_u_investment_cost.items():
                alt_ent_class = (n_to_u_alt, unit_source["entity_byname"], "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "invest_cost", alt_ent_class, n_to_u_val)
            for n_to_u_alt, n_to_u_val in n_to_u_fixed_cost.items():
                alt_ent_class = (n_to_u_alt, unit_source["entity_byname"], "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "fixed_cost", alt_ent_class, n_to_u_val)
            for n_to_u_alt, n_to_u_val in n_to_u_salvage_value.items():
                alt_ent_class = (n_to_u_alt, unit_source["entity_byname"], "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "salvage_value", alt_ent_class, n_to_u_val)

        # If no capacity nor investment_cost defined, warn.
        if not (u_to_n_capacity or u_to_n_investment_cost or n_to_u_capacity or n_to_u_investment_cost):
            print("Unit without capacity or investment_cost:" + unit_source["name"])

    return target_db


def create_timeline(source_db, target_db):
    for system_entity in source_db.get_entity_items(entity_class_name="system"):
        for param in source_db.get_parameter_value_items(entity_class_name="system", entity_name=system_entity["name"], parameter_definition_name="timeline"):
            value = api.from_database(param["value"], param["type"])
            if value.VALUE_TYPE == 'time series':
                # this works only if time resolution is <= 1 month - relativedelta does not have an easy way to calculate number of days over month boundaries
                value = api.Map([str(x) for x in value.indexes],
                                [float(x) for x in value.values + value.resolution[0].days*24 + value.resolution[0].hours + value.resolution[0].minutes/60],
                                index_name=value.index_name)
                value._value_type = "map"
            target_db = ines_transform.add_item_to_DB(target_db, "timestep_duration", [param["alternative_name"], (system_entity["name"], ), "timeline"], value)
    for solve_entity in source_db.get_entity_items(entity_class_name="solve_pattern"):
        for param_period in source_db.get_parameter_value_items(solve_entity["entity_class_name"], entity_name=solve_entity["name"], parameter_definition_name="period"):
            period = api.from_database(param_period["value"], param_period["type"])
            target_db = ines_transform.add_item_to_DB(target_db, "realized_periods",
                                         [param_period["alternative_name"], (solve_entity["name"],), "solve"], period,
                                         value_type="array")
            target_db = ines_transform.add_item_to_DB(target_db, "invest_periods",
                                         [param_period["alternative_name"], (solve_entity["name"],), "solve"], period,
                                         value_type="array")
            if param_period["type"] == "array":
                timeblock_set_array = []
                for period_array_member in period.values:
                    timeblock_set_array.append(solve_entity["name"])
                period__timeblock_set = api.Map(period.values, timeblock_set_array, index_name="period")
            else:
                period__timeblock_set = api.Map([period], [solve_entity["name"]], index_name="period")
            #print(period__timeblock_set)
            target_db = ines_transform.add_item_to_DB(target_db, "period_timeblockSet", [param_period["alternative_name"], (solve_entity["name"],), "solve"], period__timeblock_set, value_type="map")
        for param_start in source_db.get_parameter_value_items(solve_entity["entity_class_name"], entity_name=solve_entity["name"], parameter_definition_name="start_time"):
            value_start_time = api.from_database(param_start["value"], param_start["type"])
            for param_duration in source_db.get_parameter_value_items(solve_entity["entity_class_name"], entity_name=solve_entity["name"], parameter_definition_name="duration"):
                value_duration = api.from_database(param_duration["value"], param_duration["type"])
                block_duration = api.Map([str(value_start_time)], [value_duration.value.days * 24 + value_duration.value.hours + value_duration.value.minutes / 60], index_name="timestep")
                #print(block_duration)
                new_param, type_ = api.to_database(block_duration)
                added, error = target_db.add_parameter_value_item(entity_class_name="timeblockSet",
                                                                    entity_byname=(solve_entity["name"],),
                                                                    parameter_definition_name="block_duration",
                                                                    value=new_param,
                                                                    type=type_,
                                                                    alternative_name=param_start["alternative_name"]
                                                                  )
                if error:
                    print("writing block_duration failed: " + error)
                # To make sure all pairs of start and duration are captured:
                if param_start["alternative_name"] is not param_duration["alternative_name"]:
                    added, error = target_db.add_parameter_value_item(entity_class_name="timeblockSet",
                                                                        entity_byname=(solve_entity["name"],),
                                                                        parameter_definition_name="block_duration",
                                                                        value=new_param,
                                                                        type=type_,
                                                                        alternative_name=param_duration["alternative_name"]
                                                                      )
                    if error:
                        print("writing block_duration failed: " + error)
    for solve_entity in source_db.get_entity_items(entity_class_name="solve_pattern"):
        for system_entity in source_db.get_entity_items(entity_class_name="system"):
            added, error = target_db.add_item("entity",
                                                entity_class_name="timeblockSet__timeline",
                                                element_name_list=[solve_entity["name"], system_entity["name"]],
                                              )
            if error:
                print("creating entity for timeblockset__timeline failed: " + error)

    return target_db


def params_to_dict(params):
    dict_temp = {}
    for param in params:
        value = api.from_database(param["value"], param["type"])
        if value and param["type"] == 'map':
            if isinstance(value.values[0], api.Map):
                for val0 in value.values:
                    dict_temp[param["alternative_name"]] = val0.values
            else:
                dict_temp[param["alternative_name"]] = value.values
        elif value:
            dict_temp[param["alternative_name"]] = value
    return dict_temp


if __name__ == "__main__":
    main()

