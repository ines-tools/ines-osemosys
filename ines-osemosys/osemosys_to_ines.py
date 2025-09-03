import spinedb_api as api
from spinedb_api import DatabaseMapping
from pathlib import Path
try:
    from ines_tools import ines_transform
except:
    try:
        sys.path.insert(0,str(Path(__file__).parent.parent.parent / "ines-tools"/ "ines_tools"))
        import ines_transform
    except:
        print("Cannot find ines tools as an installed package or as parallel folder")
from sqlalchemy.exc import DBAPIError
import sys
import csv
from sys import exit
import yaml
import itertools
import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse


def main():
    with DatabaseMapping(url_db_in) as source_db:
        with DatabaseMapping(url_db_out, upgrade=True) as target_db:
            ## Empty the database
            target_db.purge_items('parameter_value')
            target_db.purge_items('entity')
            target_db.purge_items('alternative')
            target_db.purge_items('scenario')
            target_db.purge_items('scenario_alternative')
            target_db.purge_items('entity_alternative')
            target_db.refresh_session()
            target_db.commit_session("Purged stuff")

            source_db.fetch_all('entity_class')
            source_db.fetch_all('entity')
            source_db.fetch_all('parameter_value')
            ## Copy scenarios alternatives
            for alternative in source_db.get_alternative_items():
                target_db.add_alternative_item(name=alternative["name"])
            for scenario in source_db.get_scenario_items():
                target_db.add_scenario_item(name=scenario["name"])
            for scenario_alternative in source_db.get_scenario_alternative_items():
                target_db.add_scenario_alternative_item(scenario_name=scenario_alternative["scenario_name"],
                                                        alternative_name=scenario_alternative["alternative_name"],
                                                        rank=scenario_alternative["rank"])
            try:
                target_db.commit_session("Added alternatives and scenarios")
            except DBAPIError as e:
                print(e)
                exit("no alternatives in the source database, check the URL for the DB")

            ## Copy entities
            target_db = ines_transform.copy_entities(source_db, target_db, entities_to_copy)
            ## Create periods from years
            target_db = create_periods(source_db, target_db)
            ## Copy timeslice parameters (manual scripting)
            target_db, datetime_indexes, timeslice_indexes, year_splits = process_timeslice_data(source_db, target_db, timeslice_csv)
            ## Copy numeric parameters(source_db, target_db, parameter_transforms)
            target_db = ines_transform.transform_parameters(source_db, target_db, parameter_transforms,
                                                                        use_default=True, default_alternative="base", ts_to_map=True)
            ## Copy method parameters
            target_db = ines_transform.process_methods(source_db, target_db, parameter_methods)
            ## Copy entities to parameters
            target_db = ines_transform.copy_entities_to_parameters(source_db, target_db, entities_to_parameters)
            ## Process demands
            target_db = process_demands(source_db, target_db , datetime_indexes)
            ## Copy capacity specific parameters (manual scripting)
            target_db = process_capacities(source_db, target_db, datetime_indexes, timeslice_indexes, year_splits)
            ## Special model level parameters
            target_db = process_model_level(source_db, target_db)
            ## Process units with zero investment cost
            target_db = process_zero_investment_cost(source_db, target_db)
            ## Add constraints
            target_db = process_RE_min_constraint(source_db, target_db)
            ## Add activity constraints
            target_db = process_activity_constraints(source_db, target_db)
            ##Process emissions
            target_db = process_emissions(source_db, target_db)
            ## Process reserves
            target_db = process_reserves(source_db, target_db, timeslice_indexes)
            ## Process storages. This is done last as it takes a copy of an unit in the target db
            target_db = process_storages(source_db, target_db)
            ## Assign node types
            target_db = process_node_types(source_db, target_db)


def create_periods(source_db, target_db):
    models = source_db.get_entity_items(entity_class_name="model")
    years = source_db.get_entity_items(entity_class_name="YEAR")
    for model in models:
        added, error = target_db.add_entity_item(entity_class_name="system", name=model["name"])
        if error:
            exit("Could not add system entity to ines-db: " + error)

        added, error = target_db.add_entity_item(entity_class_name="solve_pattern", entity_byname=(model["name"],))
        if error:
            exit("Adding solve_pattern entity failed. " + error)
        period_indexes = []
        previous_year = None
        previous_alt = None
        for year in years:
            period_alts = source_db.get_entity_alternative_items(entity_class_name="YEAR", entity_name=year["name"])
            if len(period_alts) > 1:
                exit("Multiple entity_alternatives for the YEAR entities - not managed")
            if len(period_alts) == 0:
                exit("No entity_alternative for the YEAR entity " + year["name"])
            if previous_alt and previous_alt["alternative_name"] != period_alts[0]["alternative_name"]:
                exit("Different entity_alternatives for different YEARs - not managed")
            previous_alt = period_alts[0]
            if previous_year is not None:
                years_represented = int(year["name"]) - int(previous_year["name"])
                p_value, p_type = api.to_database(years_represented)
                added, error = target_db.add_parameter_value_item(entity_class_name="period",
                                                                  entity_byname=(previous_year["name"], ),
                                                                  parameter_definition_name="years_represented",
                                                                  alternative_name=previous_alt["alternative_name"],
                                                                  value=p_value,
                                                                  type=p_type)
                if error:
                    exit("Error in adding years_repsented: " + error)
            period_indexes.append(year["name"])
            previous_year = year
        if len(years) > 1:
            years_represented = int(years[-1]["name"]) - int(years[-2]["name"])  # Assumption is that the last period is as long as the second to last period
        elif len(years) == 0:
            years_represented = int(1)
        else:
            exit("No years/periods in the source data")
        p_value, p_type = api.to_database(years_represented)
        added, error = target_db.add_parameter_value_item(entity_class_name="period",
                                                          entity_byname=(previous_year["name"],),
                                                          parameter_definition_name="years_represented",
                                                          alternative_name=period_alts[0]["alternative_name"],
                                                          value=p_value,
                                                          type=p_type)
        if error:
            exit("Error in adding years_repsented: " + error)

        current_period_alt = period_alts[0]["alternative_name"]
        array_data = api.Array(period_indexes, value_type=str, index_name="period")

        p_value, p_type = api.to_database(array_data)
        added, error = target_db.add_parameter_value_item(entity_class_name="solve_pattern",
                                                          parameter_definition_name="period",
                                                          entity_byname=tuple([model["name"]]),
                                                          alternative_name=current_period_alt,
                                                          value=p_value,
                                                          type=p_type)
        if error:
            exit("Adding ines periods from OSEMOSYS years failed. " + error)
        for ea in source_db.get_entity_alternative_items(entity_class_name="model",
                                                         entity_name=model["name"]):
            added_ea, update_ea, error = target_db.add_update_entity_alternative_item(entity_class_name="solve_pattern",
                                                                    entity_byname=(model["name"],),
                                                                    alternative_name=ea["alternative_name"],
                                                                    active=ea["active"])
            if error:
                exit("Adding solve_pattern entity_alternative failed. " + error)
            added_ea, update_ea, error = target_db.add_update_entity_alternative_item(entity_class_name="system",
                                                                    entity_byname=(model["name"],),
                                                                    alternative_name=ea["alternative_name"],
                                                                    active=ea["active"])
            if error:
                exit("Adding system entity_alternative failed. " + error)
    try:
        target_db.commit_session("Added periods from YEARs to ines_db")
    except DBAPIError as e:
        print("failed to add periods")
    return target_db

def read_timeslice_data(timeslice_csv):
    out_list = list()
    try:
        with open(timeslice_csv) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            csv_header = []
            csv_data = []
            first_line = True
            for row in csv_reader:
                if first_line:
                    for item in row:
                        csv_header.append(item)
                        csv_data.append([])
                    first_line = False
                else:
                    for k, item in enumerate(row):
                        if item == '':
                            break
                        csv_data[k].append(item)
        for index_value in zip(csv_data[0],csv_data[1], csv_data[2]):
            
            try:
                # Automatically parse the date string
                date_object = parse(index_value[0])
                out_list.append((api.DateTime(date_object), index_value[1], float(index_value[2])))
            except ValueError as e:
                print(f"Error parsing date: {e}")
                sys.exit(-1)

    except FileNotFoundError:
        print("No csv data file for " + timeslice_csv)
    return out_list


def process_timeslice_data(source_db, target_db, read_separate_csv):
    model_items = source_db.get_entity_items(entity_class_name="model")
    if len(model_items) > 1:
        exit("OSeMOSYS to ines script does not handle databases with more than one model entity")
    if len(model_items) == 0:
        exit("No model entities and associated parameters")
    model_item = model_items[0]
    
    timeslices_to_time = read_timeslice_data(read_separate_csv)
    if not timeslices_to_time:
        exit("No timeslices to datetime mapping")
    datetime_indexes = list()
    for i in timeslices_to_time:
        datetime_indexes.append(i[0])
    timeslice_indexes = []
    time_durations = []
    previous_time_duration = timeslices_to_time[0][2]
    for i in timeslices_to_time:
        timeslice_indexes.append(i[1])
        time_durations.append(float(i[2]))
    # Store the model time resolution in ines_db
    p_value, p_type = api.to_database(api.Duration(relativedelta(hours=previous_time_duration)))
    added, error = target_db.add_parameter_value_item(entity_class_name="solve_pattern",
                                                      parameter_definition_name="time_resolution",
                                                      entity_byname=tuple([model_item["name"]]),
                                                      alternative_name=default_alternative,
                                                      value=p_value,
                                                      type=p_type)
    if error:
        exit("Could not add resolution parameter to ines-db: " + error)

    timeline_map = api.TimeSeriesVariableResolution(datetime_indexes, time_durations, ignore_year=False, repeat=False, index_name="timestamp")
    p_value, p_type = api.to_database(timeline_map)
    added, error = target_db.add_parameter_value_item(entity_class_name="system",
                                                      parameter_definition_name="timeline",
                                                      entity_byname=tuple([model_item["name"]]),
                                                      alternative_name=default_alternative,
                                                      value=p_value,
                                                      type=p_type)
    if error:
        exit("Could not add timeline parameter to ines-db: " + error)

    #check for gaps in time series and create blocks of continuous time series
    datetime_block_start = datetime_indexes[0]
    datetime_block_starts = []
    datetime_block_durations = []
    for k, datetime_index in enumerate(datetime_indexes[:-1]):
        if round_to_nearest_minute(datetime_indexes[k + 1].value) - round_to_nearest_minute(datetime_index.value) > datetime.timedelta(hours=int(time_durations[k])):
            datetime_block_starts.append(datetime_block_start)
            datetime_block_durations.append(api.Duration(relativedelta(datetime_index.value, datetime_block_start.value)
                                                         + relativedelta(hours=int(time_durations[k]))))
            datetime_block_start = datetime_indexes[k + 1]
    #if continous block until the end
    if len(datetime_block_starts) == 0:
        datetime_block_starts.append(datetime_block_start)
        datetime_block_durations.append(api.Duration(relativedelta(datetime_indexes[-1].value, datetime_block_start.value)))
    spine_array = api.Array(values=datetime_block_starts, index_name="datetime")
    p_value, p_type = api.to_database(spine_array)
    added, error = target_db.add_parameter_value_item(entity_class_name="solve_pattern",
                                                      parameter_definition_name="start_time",
                                                      entity_byname=(model_item["name"],),
                                                      alternative_name=default_alternative,
                                                      value=p_value,
                                                      type=p_type)
    if error:
        print("process timeblock starttimes error: " + error)
    spine_array = api.Array(values=datetime_block_durations, index_name="duration")
    p_value, p_type = api.to_database(spine_array)
    added, error = target_db.add_parameter_value_item(entity_class_name="solve_pattern",
                                                      parameter_definition_name="duration",
                                                      entity_byname=(model_item["name"],),
                                                      alternative_name=default_alternative,
                                                      value=p_value,
                                                      type=p_type)
    if error:
        print("process timeblock durations error: " + error)

    try:
        target_db.commit_session("Added starttimes and durations for timeslices")
    except DBAPIError as e:
        print("failed to add parameter values for starttimes and durations data. " + e)

    # Go through parameters that use time indexes
    year_splits =source_db.get_parameter_value_items(entity_class_name="model",
                                                          entity_name=model_item["name"],
                                                          parameter_definition_name="YearSplit")
    for year_split in year_splits:
        year_split_data = api.from_database(year_split["value"], year_split["type"])
        target_db = add_timeslice_data(source_db, target_db, year_split_data, time_durations,
                                       "REGION__FUEL", "SpecifiedDemandProfile", "node", "flow_profile",
                                       timeslice_indexes, datetime_indexes, -1.0, True)
        #target_db = add_timeslice_data(source_db, target_db, year_split_data,
        #                               "REGION__TECHNOLOGY", "CapacityFactor", "unit", "availability",
        #                               timeslice_indexes, datetime_indexes, 1.0, False)
    return target_db, datetime_indexes, timeslice_indexes, year_splits


def get_timeslice_value(year_split_data, source_param, source_class,
                       source_param_name, timeslice_indexes, datetime_indexes,
                       multiplier, scale_with_time, time_durations = None):
    profile_data = source_param["parsed_value"]
    timeslice_profiles = {}
    for s, profile_data_by_slices in enumerate(profile_data.values):
        if scale_with_time:
            timeslice_profiles[profile_data.indexes[s]] = multiplier * \
                                                            round(float(profile_data_by_slices.values[0]) /  # Note that this takes the first value from the array of years (first year)
                                                                float(year_split_data.values[s].values[0]), 6) /8760
        else:
            timeslice_profiles[profile_data.indexes[s]] = multiplier * \
                                                            round(float(profile_data_by_slices.values[0]), 6)  # Note that this takes the first value from the array of years (first year)
    datetime_profiles = []
    for t, timeslice_index in enumerate(timeslice_indexes):
        if timeslice_index not in timeslice_profiles.keys():
            print(f'Timeslice index {timeslice_index} not found in timeslice profiles for {source_class["name"]} parameter {source_param_name}')
            sys.exit(-1)
        datetime_profiles.append(timeslice_profiles[timeslice_index])
    if scale_with_time:
        datetime_profiles = [round(float(dp) * time_durations[t], 6) for t, dp in enumerate(datetime_profiles)]
    to_db_profile_data = api.TimeSeriesVariableResolution(
        datetime_indexes,
        datetime_profiles,
        ignore_year=False,
        repeat=False
    )
    profile_data_divided, p_type = api.to_database(to_db_profile_data)
    
    return profile_data_divided, p_type,

def add_timeslice_data(source_db, target_db, year_split_data, time_durations, source_class_name,
                       source_param_name, target_class_name, target_param_name, timeslice_indexes, datetime_indexes,
                       multiplier, scale_with_time):
    for source_class in source_db.get_entity_items(entity_class_name=source_class_name):
        for source_param in source_db.get_parameter_value_items(entity_class_name=source_class_name,
                                                                 entity_name=source_class["name"],
                                                                 parameter_definition_name=source_param_name):
            
            profile_data_divided, p_type = get_timeslice_value(year_split_data, source_param, source_class, source_param_name, 
                                                               timeslice_indexes, datetime_indexes, multiplier, scale_with_time, 
                                                               time_durations = time_durations)
            target_entity_byname = tuple(['__'.join(source_param["entity_byname"])])
            added, error = target_db.add_parameter_value_item(entity_class_name=target_class_name,
                                                                parameter_definition_name=target_param_name,
                                                                entity_byname=target_entity_byname,
                                                                alternative_name=source_param["alternative_name"],
                                                                value=profile_data_divided,
                                                                type=p_type)
            if error:
                print("process timeslice data error: " + error)
    try:
        target_db.commit_session("Added parameter values for timeslice data")
    except DBAPIError as e:
        print("failed to add parameter values for timeslice data")
        print(e)
    return target_db


def process_capacities(source_db, target_db, datetime_indexes, timeslice_indexes, year_splits):
    region__tech__fuel_entities = source_db.get_entity_items(entity_class_name="REGION__TECHNOLOGY__FUEL")
    TotalAnnualMaxCapacityInvestment = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY",
                                                parameter_definition_name="TotalAnnualMaxCapacityInvestment")
    TotalAnnualMinCapacityInvestment = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY",
                                                parameter_definition_name="TotalAnnualMinCapacityInvestment")
    
    source_unit_investment_cost_all = get_parameter_values_with_default(source_db, "REGION__TECHNOLOGY", "CapitalCost", use_default = True, ignore_default_value_of = None)
    source_unit_fixed_cost_all = get_parameter_values_with_default(source_db, "REGION__TECHNOLOGY", "FixedCost", use_default = True, ignore_default_value_of = None)
    source_unit_variable_cost_all = get_parameter_values_with_default(source_db, "REGION__TECHNOLOGY", "VariableCost", use_default = True, ignore_default_value_of = None)
    operational_life_all = get_parameter_values_with_default(source_db, "REGION__TECHNOLOGY", "OperationalLife", use_default = True, ignore_default_value_of = None)
    source_unit_interest_rate_all = get_parameter_values_with_default(source_db, "REGION__TECHNOLOGY", "DiscountRateIdv", use_default = True, ignore_default_value_of = None)

    for unit_source in source_db.get_entity_items(entity_class_name="REGION__TECHNOLOGY"):
        source_unit_investment_cost = [source for source in source_unit_investment_cost_all if source["entity_byname"] == unit_source["entity_byname"]]
        source_unit_fixed_cost = [source for source in source_unit_fixed_cost_all if source["entity_byname"] == unit_source["entity_byname"]]
        source_unit_variable_cost = [source for source in source_unit_variable_cost_all if source["entity_byname"] == unit_source["entity_byname"]]
        operational_life = [source for source in operational_life_all if source["entity_byname"] == unit_source["entity_byname"]]
        source_unit_interest_rate = [source for source in source_unit_interest_rate_all if source["entity_byname"] == unit_source["entity_byname"]]
        
        source_region_interest_rate = source_db.get_parameter_value_items(entity_class_name="REGION", entity_name=unit_source["entity_byname"][0], parameter_definition_name="DiscountRate")
        default_discount_rate = source_db.get_parameter_definition_item(entity_class_name="REGION", name="DiscountRate")
        unit_entity_alternatives = source_db.get_entity_alternative_items(entity_class_name="unit")

        #calculating the efficiency from InputActivityRatio and OutputActivityRatio
        act_indexes = None
        input_act_ratio = dict(list())
        output_act_ratio = dict(list())
        input_act_params = []
        output_act_params = []
        input_names = []
        output_names = []
        for rtf_ent in region__tech__fuel_entities:
            if rtf_ent["entity_byname"][0] + rtf_ent["entity_byname"][1] == unit_source["entity_byname"][0] + unit_source["entity_byname"][1]:
                temp = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__FUEL",
                                                           entity_name=rtf_ent["name"],
                                                           parameter_definition_name="InputActivityRatio")
                if temp:
                    input_act_params.extend(temp)
                    input_names.append(rtf_ent["entity_byname"])
                temp = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__FUEL",
                                                           entity_name=rtf_ent["name"],
                                                           parameter_definition_name="OutputActivityRatio")
                if temp:
                    output_act_params.extend(temp)
                    output_names.append(rtf_ent["entity_byname"])

        #pairing the alternative and the value for InputActivityRatio and OutputActivityRatio
        for param in input_act_params:
            fuel = param["entity_byname"][2]
            mode_map_objects = api.from_database(param["value"], "map")
            for k, input_map_object in enumerate(mode_map_objects.values):
                if k == 0: # taking only the first mode of operation
                    if fuel not in input_act_ratio.keys():
                        input_act_ratio[fuel] = []
                    input_act_ratio[fuel].append({param["alternative_name"]: input_map_object.values})
                    if act_indexes:
                        if not act_indexes == input_map_object.indexes:
                            exit("InputActivityRatio and/or OutputActivityRatio contain inconsistent YEAR indexes for " and rtf_ent["name"])
                    act_indexes = input_map_object.indexes
        for param in output_act_params:
            fuel = param["entity_byname"][2]
            mode_map_objects = api.from_database(param["value"], "map")
            for k, output_map_object in enumerate(mode_map_objects.values):
                if k == 0: # taking only the first mode of operation
                    if fuel not in output_act_ratio.keys():
                        output_act_ratio[fuel] = []
                    output_act_ratio[fuel].append({param["alternative_name"]: output_map_object.values})
                    if act_indexes:
                        if not act_indexes == output_map_object.indexes:
                            exit("InputActivityRatio and/or OutputActivityRatio contain inconsistent YEAR indexes")
                    act_indexes = output_map_object.indexes

        #calculating the efficiency from InputActivityRatio and OutputActivityRatio
        if len(input_act_ratio) == 1 and len(output_act_ratio) == 1:
            for alt_i, iar in next(iter(input_act_ratio.values()))[0].items():
                for alt_o, oar in next(iter(output_act_ratio.values()))[0].items():
                    alt = alternative_name_from_two(alt_i, alt_o, target_db)
                    alt_ent_class = (alt, (unit_source["name"],), "unit")
                    target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, oar[0] / iar[0])
        elif len(input_act_ratio) == 0 and len(output_act_ratio) == 1:
            pass # no eff can be defined, will use the conversion method: coefficients_only
        elif len(input_act_ratio) == 1 and len(output_act_ratio) == 0:
            pass # no eff can be defined, will use the conversion method: coefficients_only
        elif len(input_act_ratio) == 0 and len(output_act_ratio) == 0:
            print("No InputActivityRatio nor OutputActivityRatio defined for " + unit_source["name"])
            continue
        else:
            # processing multiple inputs or outputs with their alternatives
            input_list = []
            output_list = []
            alt_set = set()
            alt_list = []
            for k, input_ar in enumerate(next(iter(input_act_ratio.values()))):
                input_list.append({})
                for alt_i, iar in input_ar.items():
                    input_list[k][alt_i] = iar
                    alt_set.add(alt_i)
            for k, output_ar in enumerate(next(iter(output_act_ratio.values()))):
                output_list.append({})
                for alt_o, oar in output_ar.items():
                    output_list[k][alt_o] = oar
                    alt_set.add(alt_o)
            for k in range(len(alt_set)):
                alt_list.extend(list(itertools.combinations(alt_set, k + 1)))

            input_values = []
            output_values = []
            for alt_l in alt_list:
                alt = "__".join(alt_l)
                for inp in input_list:
                    for alti in alt_l:
                        if alti in inp:
                            input_values.append(inp[alti])
                            continue
                for out in output_list:
                    for alto in alt_l:
                        if alto in out:
                            output_values.append(out[alto])
                            continue
                if len(input_values) == len(input_list) and len(output_values) == len(output_list):
                    summed_output = [sum(x) for x in zip(*output_values)]
                    summed_input = [sum(x) for x in zip(*input_values)]
                    alt_ent_class = (alt, (unit_source["name"],), "unit")
                    target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, summed_output[0] / summed_input[0])
            if len(output_values) > 1: # if there are multiple outputs, we need to fix the ratio of the flows
                output_1 = None
                alt_1 = None
                for k, out in enumerate(output_list):
                    for alto in alt_set:
                        if alto in out:
                            if output_1:
                                ent_byname = ('__'.join([output_names[k - 1][0], output_names[k - 1][1]]),
                                              '__'.join([output_names[k - 1][0], output_names[k - 1][2]]),
                                              '__'.join([output_names[k][0], output_names[k][1]]),
                                              '__'.join([output_names[k][0], output_names[k][2]]))
                                target_db.add_entity_item(entity_class_name="unit_flow__unit_flow", entity_byname=ent_byname)
                                fix_ratio = [round(o2 / o1, 6) for o2, o1 in zip(out[alto], output_1)]
                                output_map_object.values = fix_ratio
                                alt = alternative_name_from_two(alt_1, alto, target_db)
                                alt_ent_class = (alt, ent_byname, "unit_flow__unit_flow")
                                target_db = ines_transform.add_item_to_DB(target_db, "equality_ratio", alt_ent_class, output_map_object)
                        output_1 = out[alto]
                        alt_1 = alto

        #choose if limiting input or output capacity
        if len(output_act_ratio) == 1:
            if len(next(iter(output_act_ratio.values()))) > 1:
                exit("There are more than one alternative value for input / output activity_ratio parameters - that is not handled. Unit: " + unit_source["name"])
            act_ratio_dict = next(iter(output_act_ratio.values()))[0]
            entity_byname = output_names[0]
            entity_byname = (entity_byname[0] + "__" + entity_byname[1], entity_byname[0] + "__" + entity_byname[2])
            class_name = "unit__to_node"
        elif len(input_act_ratio) == 1:
            if len(next(iter(input_act_ratio.values()))) > 1:
                exit("There are more than one alternative value for input / output activity_ratio parameters - that is not handled. Unit: " + unit_source["name"])
            act_ratio_dict = next(iter(input_act_ratio.values()))[0]
            entity_byname = input_names[0]
            entity_byname = (entity_byname[0] + "__" + entity_byname[2], entity_byname[0] + "__" + entity_byname[1])
            class_name = "node__to_unit"
        else:
            exit("Not handling multiple inputs together with multiple outputs. Error in entity: " + unit_source["name"])
        
        # Get the possible capacity of one technology unit
        source_CapacityOfOneTechnologyUnit = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="CapacityOfOneTechnologyUnit")
        if len(source_CapacityOfOneTechnologyUnit) > 1:
            exit("Multiple alternatives for CapacityOfOneTechnologyUnit - not handled")
        elif len(source_CapacityOfOneTechnologyUnit) == 0:
            cap = default_unit_size
        else:
            source_CapacityOfOneTechnologyUnit = source_CapacityOfOneTechnologyUnit[0]
            cap = api.from_database(source_CapacityOfOneTechnologyUnit["value"], "map").values[0] * capacity_unit_factor
            if any(x != cap for x in api.from_database(source_CapacityOfOneTechnologyUnit["value"].values, "map").values):
                exit("CapacityOfOneTechnologyUnit has different values for different years - not handled")
            target_db = ines_transform.add_item_to_DB(target_db, "investment_uses_integer", (unit_source["name"],), True)

        # Place the capacity, note that the act_ratio_dict contains only one alternative
        for alt_activity, act_ratio_list in act_ratio_dict.items():
            if class_name == "unit__to_node":
                unit_byname = (entity_byname[0],)
            if class_name == "node__to_unit":
                unit_byname = (entity_byname[1],)
            if any(x != act_ratio_list[0] for x in act_ratio_list):
                exit("The unit changes it's activity ratio between years - this is not handled. Entity: " + entity_byname)
            act_ratio = act_ratio_list[0]
            unit_capacity =  cap * act_ratio
            alt_ent_class = (alt_activity, entity_byname, class_name)
            target_db = ines_transform.add_item_to_DB(target_db, "capacity", alt_ent_class, unit_capacity)
        
        # Pass number of unit values
        flag_limit_cumulative_investments = False
        source_unit_residual_capacity = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="ResidualCapacity")
        for param in source_unit_residual_capacity:
            param_map = api.from_database(param["value"], "map")
            param_map.values = [x * capacity_unit_factor * act_ratio / unit_capacity for x in param_map.values]
            alt_ent_class = (param["alternative_name"], unit_byname, "unit")
            target_db = ines_transform.add_item_to_DB(target_db, "units_existing", alt_ent_class, param_map)
        source_unit_total_max = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="TotalAnnualMaxCapacity")
        for param in source_unit_total_max:
            param_map = api.from_database(param["value"], "map")
            param_map.values = [x * capacity_unit_factor * act_ratio / unit_capacity for x in param_map.values]
            alt_ent_class = (param["alternative_name"], unit_byname, "unit")
            target_db = ines_transform.add_item_to_DB(target_db, "units_max_cumulative", alt_ent_class, param_map)
            flag_limit_cumulative_investments = True
        source_unit_total_min = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="TotalAnnualMinCapacity")
        for param in source_unit_total_min:
            param_map = api.from_database(param["value"], "map")
            param_map.values = [x * capacity_unit_factor * act_ratio / unit_capacity for x in param_map.values]
            alt_ent_class = (param["alternative_name"], unit_byname, "unit")
            target_db = ines_transform.add_item_to_DB(target_db, "units_min_cumulative", alt_ent_class, param_map)
            flag_limit_cumulative_investments = True
        
        for param in TotalAnnualMaxCapacityInvestment:
            if param["entity_byname"] == unit_source["entity_byname"]:
                param_map = api.from_database(param["value"], "map")
                param_map.values = [x * capacity_unit_factor * act_ratio / unit_capacity for x in param_map.values]
                param_map.index_name = "period"
                alt_ent_class = (param["alternative_name"], unit_byname, "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "units_invest_max_period", alt_ent_class, param_map)

        for param in TotalAnnualMinCapacityInvestment:
            if param["entity_byname"] == unit_source["entity_byname"]:
                param_map = api.from_database(param["value"], "map")
                param_map.values = [x * capacity_unit_factor * act_ratio / unit_capacity for x in param_map.values]
                param_map.index_name = "period"
                alt_ent_class = (param["alternative_name"], unit_byname, "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "units_invest_min_period", alt_ent_class, param_map)
        
        for alt_activity, act_ratio in act_ratio_dict.items():    
            flag_allow_investments = False
            alt_inv_cost = alt_activity
            alt_fixed_cost = alt_activity
            for source_param in source_unit_investment_cost:
                alt = source_param["alternative_name"]
                source_param = api.from_database(source_param["value"], source_param["type"])
                if isinstance(source_param, api.Map):
                    source_param.values = [s * investment_unit_factor / a for s, a in zip(source_param.values, act_ratio)]
                    source_param.index_name = "period"
                    if max(source_param.values) > 0 and min(source_param.values) == 0.0:
                        exit("Investment cost 0 for some years and above 0 for others - don't know how to handle")
                    if min(source_param.values) > 0:
                        alt_inv_cost = alt_activity
                else:
                    if source_param > 0:
                        alt_inv_cost = alt_activity
                alt_ent_class = (alt, entity_byname, class_name)
                target_db = ines_transform.add_item_to_DB(target_db, "investment_cost", alt_ent_class, source_param)
            for source_param in source_unit_fixed_cost:
                alt = source_param["alternative_name"]
                source_param = api.from_database(source_param["value"], source_param["type"])
                if isinstance(source_param, api.Map):
                    source_param.values = [s * investment_unit_factor / a for s, a in zip(source_param.values, act_ratio)]
                    source_param.index_name = "period"
                    if max(source_param.values) > 0 and min(source_param.values) == 0.0:
                        exit("Fixed cost 0 for some years and above 0 for others - don't know how to handle")
                    if min(source_param.values) > 0:
                        alt_fixed_cost = alt_activity
                else:
                    if source_param > 0:
                        alt_fixed_cost = alt_activity
                alt_ent_class = (alt, entity_byname, class_name)
                target_db = ines_transform.add_item_to_DB(target_db, "fixed_cost", alt_ent_class, source_param)
            
            #find interest rate from either from the entity itself, region or region's default value
            interest_rate = None
            for source_param in source_unit_interest_rate:
                if source_param["entity_byname"] == unit_source["entity_byname"]:
                    interest_rate = api.from_database(source_param["value"], source_param["type"])
                    alt = source_param["alternative_name"]
            if not interest_rate:
                for source_param in source_region_interest_rate:
                    if source_param["entity_byname"][0] == unit_source["entity_byname"][0]:
                        interest_rate = api.from_database(source_param["value"], source_param["type"])
                        alt = source_param["alternative_name"]
            if not interest_rate:
                if default_discount_rate["default_value"]:
                    interest_rate = api.from_database(default_discount_rate["default_value"], default_discount_rate["default_type"])
                    for unit in unit_entity_alternatives:
                        if unit["entity_byname"] == unit_source["entity_byname"]:
                            alt = unit["entity_alternative_name"]
            if interest_rate:
                target_db = ines_transform.add_item_to_DB(target_db, "interest_rate", (alt, unit_byname, "unit"), interest_rate)

            #If lifetime exists, invesments are allowed. No costs are needed.
            for source_param in operational_life:
                operational_life_value = api.from_database(source_param["value"], source_param["type"])
                if operational_life_value and operational_life_value > 0:
                    flag_allow_investments = True

            if flag_allow_investments:
                if flag_limit_cumulative_investments:
                    p_value, p_type = api.to_database("cumulative_limits")
                else:
                    p_value, p_type = api.to_database("no_limits")
                
            else:
                p_value, p_type = api.to_database("not_allowed")
            alt = alternative_name_from_two(alt_inv_cost, alt_fixed_cost, target_db)
            added, error = target_db.add_parameter_value_item(entity_class_name="unit",
                                                              entity_byname=unit_byname,
                                                              parameter_definition_name="investment_method",
                                                              alternative_name=alt,  # This is not really satisfactory, if there are values across different alternatives. Tries to do something, but it's shaky.
                                                              value=p_value,
                                                              type=p_type)
            if error:
                exit("error in trying to add investment_method: " + error)
            for source_param in source_unit_variable_cost:
                # Not doing this, since it's messy, instead exiting above if more than one act_ratio_dict:
                # alt = alternative_name_from_two(source_param["alternative_name"], alt_activity, target_db)
                alt = source_param["alternative_name"]
                source_param = api.from_database(source_param["value"], source_param["type"])
                if isinstance(source_param, api.Map):
                    if isinstance(source_param.values[0], api.Map):
                        print("Only one mode_of_operation is allowed, taking the first one.")
                        source_param = source_param.values[0]  # Bypass mode_of_operation dimension (assume there is only one)
                    source_param.values = [s * variable_cost_unit_factor / a / 8760 for s, a in zip(source_param.values, act_ratio)]
                    source_param.index_name = "period"
                alt_ent_class = (alt, entity_byname, class_name)
                target_db = ines_transform.add_item_to_DB(target_db, "other_operational_cost", alt_ent_class, source_param)

        #add capacity factor
        for source_capacity_factor in source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="CapacityFactor"):
            for year_split in year_splits:
                year_split_data = api.from_database(year_split["value"], year_split["type"])
                profile_data_divided, p_type = get_timeslice_value(year_split_data, source_capacity_factor, "REGION__TECHNOLOGY", "CapacityFactor",
                                                                    timeslice_indexes, datetime_indexes, 1.0, False)
                added, error = target_db.add_parameter_value_item(entity_class_name=class_name,
                                                        parameter_definition_name="profile_limit_upper",
                                                        entity_byname=entity_byname,
                                                        alternative_name=source_capacity_factor["alternative_name"],
                                                        value=profile_data_divided,
                                                        type=p_type)
            target_db = ines_transform.add_item_to_DB(target_db, "profile_method", alt_ent_class, "upper_limit")


        if (len(output_act_ratio) == 1 and len(input_act_ratio) == 0) or (len(output_act_ratio) == 0 and len(input_act_ratio) == 1):
            for alt_activity, act_ratio in act_ratio_dict.items():
                alt_ent_class = (alt_activity, unit_byname, "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "conversion_method", alt_ent_class, "coefficients_only")
        if len(output_act_ratio) > 0 and len(input_act_ratio) > 0:
            for alt_activity, act_ratio in act_ratio_dict.items():
                alt_ent_class = (alt_activity, unit_byname, "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "conversion_method", alt_ent_class, "constant_efficiency")
        # If no capacity nor investment_cost defined, warn.
        if not (source_unit_residual_capacity or source_unit_investment_cost):
            print("Unit without capacity or investment_cost:" + unit_source["name"])
    
    try:
        target_db.commit_session("Added capacity related parameter values")
    except:
        print("failed to add capacity related parameter values")

    return target_db


def params_to_dict(params):
    dict_temp = {}
    for param in params:
        value = api.from_database(param["value"], param["type"])
        if value and param["type"] == 'map':
            if isinstance(value.values[0], api.Map):
                for val0 in value.values:
                    dict_temp[param["alternative_name"]] = [val0.indexes, val0.values]
            else:
                dict_temp[param["alternative_name"]] = [value.indexes, value.values]
        elif value:
            dict_temp[param["alternative_name"]] = value
    return dict_temp


def process_model_level(source_db, target_db):
    discount_rate = source_db.get_parameter_definition_item(entity_class_name="REGION",
                                                            name="DiscountRate")
    model_entities = source_db.get_entity_items(entity_class_name="model")
    for model_entity in model_entities:
        model_entity_alternatives = source_db.get_entity_alternative_items(entity_class_name="model",
                                                                           entity_name=model_entity["name"])
        for model_entity_alternative in model_entity_alternatives:
            added, error = target_db.add_parameter_value_item(entity_class_name="system",
                                                              entity_byname=(model_entity["name"], ),
                                                              parameter_definition_name="discount_rate",
                                                              alternative_name=model_entity_alternative["alternative_name"],
                                                              type=discount_rate["default_type"],
                                                              value=discount_rate["default_value"])
            if error:
                exit("Error when trying to add discount_rate: " + error)

    try:
        target_db.commit_session("Added special model level parameters")
    except:
        print("failed to add special model level parameters")

    return target_db


def process_zero_investment_cost(source_db, target_db):
    units = source_db.get_entity_items(entity_class_name="REGION__TECHNOLOGY")
    alts = source_db.get_alternative_items()
    for alt in alts:
        for unit in units:
            unit_alternatives = source_db.get_entity_alternative_items(entity_class_name="TECHNOLOGY",
                                                                       entity_byname=(unit["element_name_list"][1], ))
            if not any(unit_alt["alternative_name"] == alt["name"] and unit_alt["active"] is True for unit_alt in unit_alternatives):
                continue

            flag_invest_zero = False
            flag_fixed_zero = False
            flag_existing_zero = False
            flag_operational_life_zero = False
            invest_cost = source_db.get_parameter_value_item(entity_class_name="REGION__TECHNOLOGY",
                                                             entity_byname=unit["entity_byname"],
                                                             alternative_name=alt["name"],
                                                             parameter_definition_name="CapitalCost")
            fixed_cost = source_db.get_parameter_value_item(entity_class_name="REGION__TECHNOLOGY",
                                                            entity_byname=unit["entity_byname"],
                                                            alternative_name=alt["name"],
                                                            parameter_definition_name="FixedCost")
            existing = source_db.get_parameter_value_item(entity_class_name="REGION__TECHNOLOGY",
                                                          entity_byname=unit["entity_byname"],
                                                          alternative_name=alt["name"],
                                                          parameter_definition_name="ResidualCapacity")
            operational_life = source_db.get_parameter_value_item(entity_class_name="REGION__TECHNOLOGY",
                                                          entity_byname=unit["entity_byname"],
                                                          alternative_name=alt["name"],
                                                          parameter_definition_name="OperationalLife")
            
            if not invest_cost:
                param_def_item = source_db.get_parameter_definition_item(entity_class_name="REGION__TECHNOLOGY", name=unit["entity_byname"]) 
                if param_def_item and param_def_item["default_value"] and param_def_item["default_value"] > 0:
                    invest_cost = param_def_item["default_value"]
            if not fixed_cost:
                param_def_item = source_db.get_parameter_definition_item(entity_class_name="REGION__TECHNOLOGY", name=unit["entity_byname"]) 
                if param_def_item and param_def_item["default_value"] and param_def_item["default_value"] > 0:
                    fixed_cost = param_def_item["default_value"]
            if not existing:
                param_def_item = source_db.get_parameter_definition_item(entity_class_name="REGION__TECHNOLOGY", name=unit["entity_byname"]) 
                if param_def_item and param_def_item["default_value"] and param_def_item["default_value"] > 0:
                    existing = param_def_item["default_value"]
            if not operational_life:
                param_def_item = source_db.get_parameter_definition_item(entity_class_name="REGION__TECHNOLOGY", name=unit["entity_byname"]) 
                if param_def_item and param_def_item["default_value"] and param_def_item["default_value"] > 0:
                    operational_life = param_def_item["default_value"]

            if invest_cost:
                for i in invest_cost["parsed_value"].values:
                    if i == 0:
                        flag_invest_zero = True
                        break
            else:
                flag_invest_zero = True
            if fixed_cost:
                for f in fixed_cost["parsed_value"].values:
                    if f == 0:
                        flag_fixed_zero = True
                        break
            else:
                flag_fixed_zero = True
            if existing:
                for e in existing["parsed_value"].values:
                    if e == 0:
                        flag_existing_zero = True
                        break
            else:
                flag_existing_zero = True
            if operational_life:
                for e in existing["parsed_value"].values:
                    if e == 0:
                        flag_operational_life_zero = True
                        break
            else:
                flag_operational_life_zero = True

            if flag_existing_zero and flag_operational_life_zero:
                break #if no operational life, unit cannot be invested in osemosys, if also no residual capacity, the unit does not exist.

            if flag_invest_zero and flag_fixed_zero and flag_existing_zero:
                variable_cost = source_db.get_parameter_value_item(entity_class_name="REGION__TECHNOLOGY",
                                                                   entity_byname=unit["entity_byname"],
                                                                   alternative_name=alt["name"],
                                                                   parameter_definition_name="VariableCost")
                p_value, p_type = api.to_database(unlimited_unit_capacity / default_unit_size)
                added, updated, error = target_db.add_update_parameter_value_item(entity_class_name="unit",
                                                                                  entity_byname=(unit["name"],),
                                                                                  alternative_name=alt["name"],
                                                                                  parameter_definition_name="units_existing",
                                                                                  type=p_type,
                                                                                  value=p_value)
                if error:
                    exit("Failed to add existing capacity for a unit without investment cost or existing capacity: " + error)

                if not variable_cost:
                    print("Warning: unit " + unit["name"] + " does not have investment cost, existing capacity nor variable cost in alternative " + alt["name"] + ". Maybe not limited.")
                    continue
                variable_cost_list = variable_cost["parsed_value"].values[0].values
                # If unit has variable cost higher than the penalty boundary setting, then move the variable cost to penalty costs
                if max(variable_cost_list) >= unit_to_penalty_boundary:
                    added, updated, error = target_db.add_update_entity_alternative_item(entity_class_name="unit",
                                                                                         entity_byname=(unit["name"],),
                                                                                         alternative_name=alt["name"],
                                                                                         active=False)
                    if error:
                        exit("Failed to inactivate unit that was being turned into node penalty cost: " + error)
                    unit__nodes = source_db.get_entity_items(entity_class_name="REGION__TECHNOLOGY__FUEL")
                    for unit__node in unit__nodes:
                        if unit__node["entity_byname"][0:2] == unit["entity_byname"]:
                            oa_ratio = source_db.get_parameter_value_item(entity_class_name="REGION__TECHNOLOGY__FUEL",
                                                                          entity_byname=unit__node["entity_byname"],
                                                                          alternative_name=alt["name"],
                                                                          parameter_definition_name="OutputActivityRatio")
                            node_name = unit__node["entity_byname"][0] + "__" + unit__node["entity_byname"][2]
                            # Ignore mode of operation and just take the output activity ratios
                            oa_ratio_list = oa_ratio["parsed_value"].values[0].values
                            penalty_up = [oa * var for oa, var in zip(oa_ratio_list, variable_cost_list)]
                            penalty_up_map = api.Map(indexes=oa_ratio["parsed_value"].values[0].indexes,
                                                          values=penalty_up,
                                                          index_name="period")
                            p_value, p_type = api.to_database(penalty_up_map)
                            added, updated, error = target_db.add_update_parameter_value_item(entity_class_name="node",
                                                                                              entity_byname=(node_name,),
                                                                                              alternative_name=alt["name"],
                                                                                              parameter_definition_name="penalty_upward",
                                                                                              type=p_type,
                                                                                              value=p_value)
                            if error:
                                exit(
                                    "Failed to add penalty price based on the variable cost of a unit without investment cost or existing capacity but node with demand: " + error)
    try:
        target_db.commit_session("Inactivated units without investment costs and existing capacity. Instead use commodity price of the node")
    except:
        print("failed to process units without investment costs and existing capacity")
    return target_db

def process_demands(source_db, target_db, datetime_indexes):

    region__fuels = source_db.get_entity_items(entity_class_name="REGION__FUEL")
    AccumulatedAnnualDemand = source_db.get_parameter_value_items(entity_class_name="REGION__FUEL", parameter_definition_name="AccumulatedAnnualDemand")    
    SpecifiedAnnualDemand = source_db.get_parameter_value_items(entity_class_name="REGION__FUEL", parameter_definition_name="SpecifiedAnnualDemand")

    for region_fuel in region__fuels:
        for param in AccumulatedAnnualDemand:
            if param["entity_byname"] == region_fuel["entity_byname"]:
                alt_ent_class = [param["alternative_name"], (param["entity_byname"][0]+"__"+param["entity_byname"][1],), "node"] 
                param_map = api.from_database(param["value"], param["type"])
                if isinstance(param_map, float):
                    target_db = ines_transform.add_item_to_DB(target_db, "flow_annual", alt_ent_class, -param_map)
                else:
                    param_map.values = [-x * demand_unit_factor  for x in param_map.values]
                    target_db = ines_transform.add_item_to_DB(target_db, "flow_annual", alt_ent_class, param_map)
                
        for param in SpecifiedAnnualDemand:
            if param["entity_byname"] == region_fuel["entity_byname"]:
                alt_ent_class = [param["alternative_name"], (param["entity_byname"][0]+"__"+param["entity_byname"][1],), "node"] 
                param_map = api.from_database(param["value"], param["type"])
                if isinstance(param_map, float):
                    target_db = ines_transform.add_item_to_DB(target_db, "flow_annual", alt_ent_class, param_map)
                else:
                    param_map.values = [x * demand_unit_factor  for x in param_map.values]
                    target_db = ines_transform.add_item_to_DB(target_db, "flow_annual", alt_ent_class, param_map)
                target_db = ines_transform.add_item_to_DB(target_db, "flow_scaling_method", alt_ent_class, "scale_to_annual")
        
    return target_db

def process_storages(source_db, target_db):

    ## Create relationships. OSEMOSYS can have the same technology charging and discharging a storage.
    region__storages = source_db.get_entity_items(entity_class_name="REGION__STORAGE")
    technologys = source_db.get_entity_items(entity_class_name="TECHNOLOGY")
    TechnologyFromStorage = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__STORAGE",
                                                parameter_definition_name="TechnologyFromStorage")
    TechnologyToStorage = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__STORAGE",
                                                parameter_definition_name="TechnologyToStorage")
    StorageLevelStart = get_parameter_values_with_default(source_db, "REGION__STORAGE", "StorageLevelStart", use_default = True, ignore_default_value_of = 0.0)
    ResidualStorageCapacity = get_parameter_values_with_default(source_db, "REGION__STORAGE", "ResidualStorageCapacity", use_default = True, ignore_default_value_of = 0.0)
    CapitalCostStorage = get_parameter_values_with_default(source_db, "REGION__STORAGE", "CapitalCostStorage", use_default = True, ignore_default_value_of = 0.0)
    MinStorageCharge = get_parameter_values_with_default(source_db, "REGION__STORAGE", "MinStorageCharge", use_default = True, ignore_default_value_of = 0.0)
    StorageMaxChargeRate = get_parameter_values_with_default(source_db, "REGION__STORAGE", "StorageMaxChargeRate", use_default = True, ignore_default_value_of = 0.0)
    StorageMaxDischargeRate = get_parameter_values_with_default(source_db, "REGION__STORAGE", "StorageMaxDischargeRate", use_default = True, ignore_default_value_of = 0.0)
    DiscountRateStorage = get_parameter_values_with_default(source_db, "REGION__STORAGE", "DiscountRateStorage", use_default = True, ignore_default_value_of = 0.0)
    
    for rs in region__storages:
        storage_capacity = None
        for technology in technologys:
            fromS = False
            toS = False
            for TechFS in TechnologyFromStorage: 
                if TechFS["entity_byname"][1] == technology["entity_byname"][0] and TechFS["entity_byname"][0] == rs["entity_byname"][0] and TechFS["entity_byname"][2] == rs["entity_byname"][1]:
                    fromS = True
            for TechTS in TechnologyToStorage: 
                if TechTS["entity_byname"][1] == technology["entity_byname"][0] and TechTS["entity_byname"][0] == rs["entity_byname"][0] and TechTS["entity_byname"][2] == rs["entity_byname"][1]:
                    toS = True
            unit_name = f'{TechFS["entity_byname"][0]+"__"+TechFS["entity_byname"][1]}'
            
            if fromS and toS:
                unit_conversion_method = "two_way_linear"
                p_value, p_type = api.to_database(unit_conversion_method)
                added, updated, error = target_db.add_update_parameter_value_item(entity_class_name="unit",
                                                                    entity_byname=(unit_name,),
                                                                    alternative_name=TechFS["alternative_name"],
                                                                    parameter_definition_name="conversion_method",
                                                                    type=p_type,
                                                                    value=p_value)   
            if fromS:
                # add node_toUnit relationship
                entity_byname = (rs["entity_byname"][0] + "__" + rs["entity_byname"][1], unit_name)
                ines_transform.assert_success(target_db.add_entity_item(entity_class_name='node__to_unit', entity_byname=entity_byname), warn=True)
            if toS:
                # add unit_toNode relationship
                entity_byname = (unit_name, rs["entity_byname"][0] + "__" + rs["entity_byname"][1])
                ines_transform.assert_success(target_db.add_entity_item(entity_class_name='unit__to_node', entity_byname=entity_byname), warn=True)
    
        for param in ResidualStorageCapacity:
            if param["entity_byname"] == rs["entity_byname"]:
                alt_ent_class = (param["alternative_name"], (param["entity_byname"][0]+"__"+param["entity_byname"][1],), "node")
                param_map = api.from_database(param["value"], param["type"])
                if isinstance(param_map, float):
                    target_db = ines_transform.add_item_to_DB(target_db, "storage_capacity", alt_ent_class, param_map * storage_unit_factor)
                    target_db = ines_transform.add_item_to_DB(target_db, "storages_existing", alt_ent_class, 1.0)
                    storage_capacity = param_map
                else:
                    storage_capacity = param_map.values[0]
                    param_map.values = [x / storage_capacity for x in param_map.values]
                    target_db = ines_transform.add_item_to_DB(target_db, "storage_capacity", alt_ent_class, storage_capacity * storage_unit_factor)
                    target_db = ines_transform.add_item_to_DB(target_db, "storages_existing", alt_ent_class, param_map)
                    storage_capacity = storage_capacity

        for param in StorageLevelStart:
            if param["entity_byname"] == rs["entity_byname"]:
                alt_ent_class = (param["alternative_name"], (param["entity_byname"][0]+"__"+param["entity_byname"][1],), "node")
                param_float = api.from_database(param["value"], param["type"])
                if isinstance(param_float, float) and storage_capacity:
                    target_db = ines_transform.add_item_to_DB(target_db, "storage_state_fix", alt_ent_class, param_float/storage_capacity)
                    target_db = ines_transform.add_item_to_DB(target_db, "storage_state_fix_method", alt_ent_class, "fix_start")

    for param in CapitalCostStorage:
        alt_ent_class = (param["alternative_name"], (param["entity_byname"][0]+"__"+param["entity_byname"][1],), "node")
        param_map = api.from_database(param["value"], param["type"])
        if isinstance(param_map, float):
            target_db = ines_transform.add_item_to_DB(target_db, "storage_investment_cost", alt_ent_class, param_map * storage_investment_unit_factor)
        else:
            param_map.values = [x * storage_investment_unit_factor for x in param_map.values]
            target_db = ines_transform.add_item_to_DB(target_db, "storage_investment_cost", alt_ent_class, param_map)

    for param in MinStorageCharge:
        alt_ent_class = (param["alternative_name"], (param["entity_byname"][0]+"__"+param["entity_byname"][1],), "node")
        param_map = api.from_database(param["value"], param["type"])
        target_db = ines_transform.add_item_to_DB(target_db, "storage_state_lower_limit", alt_ent_class, param_map)

    for param in StorageMaxChargeRate:
        #create set
        set_name = f"set_charge_{param["entity_byname"][0]}_{param["entity_byname"][1]}"
        target_db = add_entity_and_entity_alternative(target_db, 'set', (set_name,), param["alternative_name"])
        constant_value =  api.from_database(param["value"], param["type"])
        target_db = ines_transform.add_item_to_DB(target_db, "flow_max_instant", [param["alternative_name"],(set_name,),'set'], constant_value * capacity_unit_factor) 
        #add flows to the set
        for TechTS in TechnologyToStorage:
            entity_byname = TechTS["entity_byname"]
            if TechTS["entity_byname"][0] == param["entity_byname"][0] and TechTS["entity_byname"][2] == param["entity_byname"][1]:
                ines_transform.assert_success(target_db.add_entity_item(entity_class_name='set__unit_flow', 
                                                                        entity_byname=(set_name, entity_byname[0] + "__" + entity_byname[1], entity_byname[0] + "__" + entity_byname[2])), 
                                                                        warn=True)
        
    for param in StorageMaxDischargeRate:
        #create set
        set_name = f"set_discharge_{param["entity_byname"][0]}_{param["entity_byname"][1]}"
        target_db = add_entity_and_entity_alternative(target_db, 'set', (set_name,), param["alternative_name"])
        constant_value =  api.from_database(param["value"], param["type"])
        target_db = ines_transform.add_item_to_DB(target_db, "flow_max_instant", [param["alternative_name"],(set_name,),'set'], constant_value* capacity_unit_factor) 
        #add flows to the set
        for TechFS in TechnologyFromStorage:
            entity_byname = TechFS["entity_byname"]
            if TechFS["entity_byname"][0] == param["entity_byname"][0] and TechFS["entity_byname"][2] == param["entity_byname"][1]:
                ines_transform.assert_success(target_db.add_entity_item(entity_class_name='set__unit_flow', 
                                                                        entity_byname=(set_name, entity_byname[0] + "__" + entity_byname[2], entity_byname[0] + "__" + entity_byname[1])), 
                                                                        warn=True)
    
    for param in DiscountRateStorage:
        alt_ent_class = (param["alternative_name"], (param["entity_byname"][0]+"__"+param["entity_byname"][1],), "node")
        param_float = api.from_database(param["value"], param["type"])
        if not param_float:
            param_float = default_interest_rate
        target_db = ines_transform.add_item_to_DB(target_db, "storage_interest_rate", alt_ent_class, param_float)

    return target_db

def process_reserves(source_db, target_db, timeslice_indexes):

    # This constraint would be possible with user constraints:
    # sum(flow_from_the_nodes) <= reserve_margin * sum(capacity of the units)
    # It would be messy and most models do not have user constraints

    # The problem is the tags for both the technology and the fuel. 
    # This makes the constraint too broad, because any set flows can be restricted by any set capcacity in the same region
    # If one would assume that all technologies and fuels are tagged, this would be possible to do with the capacity margin param

    return target_db

def process_emissions(source_db, target_db):

    EmissionActivityRatio = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__EMISSION",parameter_definition_name="EmissionActivityRatio")
    EmissionsPenalty = source_db.get_parameter_value_items(entity_class_name="REGION__EMISSION",parameter_definition_name="EmissionsPenalty")
    AnnualExogenousEmission = source_db.get_parameter_value_items(entity_class_name="REGION__EMISSION",parameter_definition_name="AnnualExogenousEmission")
    AnnualEmissionLimit = source_db.get_parameter_value_items(entity_class_name="REGION__EMISSION",parameter_definition_name="AnnualEmissionLimit")
    ModelPeriodExogenousEmission = source_db.get_parameter_value_items(entity_class_name="REGION__EMISSION",parameter_definition_name="ModelPeriodExogenousEmission")
    ModelPeriodEmissionLimit = source_db.get_parameter_value_items(entity_class_name="REGION__EMISSION",parameter_definition_name="ModelPeriodEmissionLimit")

    output_act_ratios = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__FUEL", parameter_definition_name="OutputActivityRatio")

    for param in EmissionActivityRatio:
        param_map = api.from_database(param["value"], param["type"])
        if isinstance(param_map, api.Map):
            print("INES supports only constant emission rates, taking the first value of the map")
            if isinstance(param_map.values[0], api.Map):
                param_map = param_map.values[0].values[0]
            else:
                param_map = param_map.values[0]
        
        #check if co2 or not
        if any(x in param["entity_byname"][2] for x in ["CO2", "co2", "C02"]):
            #add commodity node
            param_name = "co2_content"
            for oa_ratio in output_act_ratios:
                if oa_ratio["entity_byname"][0] == param["entity_byname"][0] and oa_ratio["entity_byname"][1] == param["entity_byname"][1]:
                    oa_ratio_val = oa_ratio["parsed_value"].values[0].values[0]
                    node_name = f'{oa_ratio["entity_byname"][1]}_CO2_commodity'
                    target_db = add_entity_and_entity_alternative(target_db, 'node', (node_name,), param["alternative_name"])
                    entity_byname = (node_name, oa_ratio["entity_byname"][0] + "__" + oa_ratio["entity_byname"][1],)
                    ines_transform.assert_success(target_db.add_entity_item(entity_class_name='node__to_unit', entity_byname=entity_byname), warn=True)
                    alt_ent_class = (param["alternative_name"], (node_name,), "node")
                    target_db = ines_transform.add_item_to_DB(target_db, param_name, alt_ent_class, param_map*oa_ratio_val)
                    target_db = ines_transform.add_item_to_DB(target_db, "node_type", alt_ent_class, "commodity")
        else:
            if any(x in param["entity_byname"][2] for x in ["NOX", "nox"]):
                param_name = "nox_emission_rate"
            elif any(x in param["entity_byname"][2] for x in ["SO2", "so2", "S02"]):
                param_name = "so2_emission_rate"
            else:
                continue
            for oa_ratio in output_act_ratios:
                if oa_ratio["entity_byname"][0] == param["entity_byname"][0] and oa_ratio["entity_byname"][1] == param["entity_byname"][1]:
                    oa_ratio_val = oa_ratio["parsed_value"].values[0].values[0]
                    entity_byname = (oa_ratio["entity_byname"][0] + "__" + oa_ratio["entity_byname"][1], oa_ratio["entity_byname"][0] + "__" + oa_ratio["entity_byname"][2])
                    alt_ent_class = (param["alternative_name"], entity_byname, "unit__to_node")
                    target_db = ines_transform.add_item_to_DB(target_db, param_name, alt_ent_class, param_map*oa_ratio_val)

    for param in EmissionsPenalty:
        if any(x in param["entity_byname"][1] for x in ["CO2", "co2", "C02"]):
            param_name = "co2_price"
        elif any(x in param["entity_byname"][1] for x in ["NOX", "nox"]):
            param_name = "nox_price"
        elif any(x in param["entity_byname"][1] for x in ["SO2", "so2", "S02"]):
            param_name = "so2_price"
        else:
            continue
        param_map = api.from_database(param["value"], param["type"])
        alt_ent_class = (param["alternative_name"], (param["entity_byname"][0],), "set")
        target_db = ines_transform.add_item_to_DB(target_db, param_name, alt_ent_class, param_map)

    for param in AnnualEmissionLimit:
        if any(x in param["entity_byname"][1] for x in ["CO2", "co2", "C02"]):
            param_name = "co2_max_period"
        elif any(x in param["entity_byname"][1] for x in ["NOX", "nox"]):
            param_name = "nox_max_period"
        elif any(x in param["entity_byname"][1] for x in ["SO2", "so2", "S02"]):
            param_name = "so2_max_period"
        else:
            continue
        param_map = api.from_database(param["value"], param["type"])
        alt_ent_class = (param["alternative_name"], (param["entity_byname"][0],), "set")
        #subract exogenous emissions, as they are not in INES spec
        for Exo in AnnualExogenousEmission:
            if Exo["entity_byname"][0] == param["entity_byname"][0] and Exo["entity_byname"][1] == param["entity_byname"][1]:
                exo_map = api.from_database(Exo["value"], Exo["type"])
                if isinstance(exo_map, api.Map):
                    exo_values = exo_map.values
                    param_map.values = [x - y for x, y in zip(param_map.values, exo_values)]
                elif isinstance(exo_map, float):
                    param_map.values = [x-exo_map for x in param_map.values]
        target_db = ines_transform.add_item_to_DB(target_db, param_name, alt_ent_class, param_map)

    for param in ModelPeriodEmissionLimit:
        if any(x in param["entity_byname"][1] for x in ["CO2", "co2", "C02"]):
            param_name = "co2_max_cumulative"
        elif any(x in param["entity_byname"][1] for x in ["NOX", "nox"]):
            param_name = "nox_max_cumulative"
        elif any(x in param["entity_byname"][1] for x in ["SO2", "so2", "S02"]):
            param_name = "so2_max_cumulative"
        else:
            continue
        param_float = api.from_database(param["value"], param["type"])
        if isinstance(param_float, float):
            alt_ent_class = (param["alternative_name"], (param["entity_byname"][0],), "set")
            for Exo in ModelPeriodExogenousEmission:
                if Exo["entity_byname"][0] == param["entity_byname"][0] and Exo["entity_byname"][1] == param["entity_byname"][1]:
                    exo_float = api.from_database(Exo["value"], Exo["type"])
                    param_float = param_float - exo_float
            target_db = ines_transform.add_item_to_DB(target_db, param_name, alt_ent_class, param_float)
    return target_db    

def process_RE_min_constraint(source_db, target_db):

    #this constraint is presented as the minimum production of demand, not of all production. 
    #They are not exactly the same constraint, but on a system without slacks, they should be the same.
    RETagTechnology = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", parameter_definition_name="RETagTechnology")
    RETagFuel = source_db.get_parameter_value_items(entity_class_name="REGION__FUEL", parameter_definition_name="RETagFuel")
    REMinProductionTarget = source_db.get_parameter_value_items(entity_class_name="REGION", parameter_definition_name="REMinProductionTarget")
    oa_ratio = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__FUEL", parameter_definition_name="OutputActivityRatio")
    SpecifiedAnnualDemand = source_db.get_parameter_value_items(entity_class_name="REGION__FUEL", parameter_definition_name="SpecifiedAnnualDemand")
    AccumulatedAnnualDemand = source_db.get_parameter_value_items(entity_class_name="REGION__FUEL", parameter_definition_name="AccumulatedAnnualDemand")

    for target in REMinProductionTarget:
        set_name = target["entity_byname"][0] + "_RE_target"
        target_db = add_entity_and_entity_alternative(target_db, 'set', (set_name,), target["alternative_name"])
        factor_map = api.from_database(target["value"], target["type"])
        yearly_demand = [0 for i in factor_map.indexes]

        for fuel in RETagFuel:
            if fuel["entity_byname"][0] == target["entity_byname"][0]:
                node_name = fuel["entity_byname"][0] + "__" + fuel["entity_byname"][1]
                #getting demand
                for param in SpecifiedAnnualDemand:
                    if param["entity_byname"] == fuel["entity_byname"]:
                        param_map = api.from_database(param["value"], param["type"])
                        for i, val in enumerate(param_map.values):
                            yearly_demand[i] += val * demand_unit_factor
                    demand = True
                if not demand:
                    for param in AccumulatedAnnualDemand:
                        if param["entity_byname"] == fuel["entity_byname"]:
                            param_map = api.from_database(param["value"], param["type"])
                            for i, val in enumerate(param_map.values):
                                yearly_demand[i] += val * demand_unit_factor
                #adding the flows to the set
                for tech in RETagTechnology:
                    if tech["entity_byname"][0] != target["entity_byname"][0]:
                        continue
                    for param in oa_ratio:
                        if param["entity_byname"][0] == target["entity_byname"][0] and \
                            param["entity_byname"][1] == tech["entity_byname"][1] and \
                            param["entity_byname"][2] == fuel["entity_byname"][1]:
                            unit_name = tech["entity_byname"][0] + "__" + tech["entity_byname"][1]
                            unit__to_node_byname = (unit_name, node_name)
                            ines_transform.assert_success(target_db.add_entity_item(entity_class_name='set__unit_flow', 
                                                                                    entity_byname=(set_name, unit__to_node_byname[0],unit__to_node_byname[1])), warn=True)
        
        flow_target_values = [x * factor for (x,factor) in zip(yearly_demand, factor_map.values)]
        flow_target = api.Map(factor_map.indexes, flow_target_values, index_name="period")
        target_db = ines_transform.add_item_to_DB(target_db, "flow_min_cumulative", [target["alternative_name"], (set_name,),'set'], flow_target)

    return target_db

def process_activity_constraints(source_db, target_db):

    TotalTechnologyAnnualActivityLowerLimit = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", parameter_definition_name="TotalTechnologyAnnualActivityLowerLimit")
    TotalTechnologyAnnualActivityUpperLimit = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", parameter_definition_name="TotalTechnologyAnnualActivityUpperLimit")
    TotalTechnologyModelPeriodActivityLowerLimit = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", parameter_definition_name="TotalTechnologyModelPeriodActivityLowerLimit")
    TotalTechnologyModelPeriodActivityUpperLimit = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", parameter_definition_name="TotalTechnologyModelPeriodActivityUpperLimit")
    oa_ratio = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY__FUEL", parameter_definition_name="OutputActivityRatio")
    
    for unit_source in source_db.get_entity_items(entity_class_name="REGION__TECHNOLOGY"):
        # Get the CapacitytoActivityRatio, the activity is energy in year, flow is power
        source_CapacitytoActivityRatio = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="CapacityToActivityUnit")
        if len(source_CapacitytoActivityRatio) > 1:
            exit("Multiple alternatives for CapacitytoActivityRatio - not handled")
        elif len(source_CapacitytoActivityRatio) == 0:
            capacity_to_activity_ratio = 1
        else:
            source_CapacitytoActivityRatio = source_CapacitytoActivityRatio[0]
            capacity_to_activity_ratio = api.from_database(source_CapacitytoActivityRatio["value"], "float")
    
        for param in TotalTechnologyAnnualActivityLowerLimit:
            if param["entity_byname"] != unit_source["entity_byname"]:
                continue
            param_map = api.from_database(param["value"], param["type"])
            set_name = param["entity_byname"][1] + "_min_annual_activity"
            target_db = add_entity_and_entity_alternative(target_db, 'set', (set_name,), param["alternative_name"])
            
            for oa in oa_ratio:
                if oa["entity_byname"][0] == param["entity_byname"][0] and oa["entity_byname"][1] == param["entity_byname"][1]:
                    unit_name = oa["entity_byname"][0] + "__" + oa["entity_byname"][1]
                    node_name = oa["entity_byname"][0] + "__" + oa["entity_byname"][2]
                    ines_transform.assert_success(target_db.add_entity_item(entity_class_name='set__unit_flow', 
                                                                            entity_byname=(set_name, unit_name, node_name)), warn=True)
                    oa_ratio_map = api.from_database(oa["value"], oa["type"])
                    for i, val in enumerate(param_map.indexes):
                        for j, oa_val in enumerate(oa_ratio_map.values[0].indexes):
                            if val == oa_val:
                                # Note that the activity is in annual energy, the flow is in power -> capacity_to_activity_ratio converts both energy to power and annual to instant
                                # Convert activity to:
                                # 1. flow
                                # 2. From energy to power
                                # 3. Power to MW
                                # 4. From instant to annual
                                param_map.values[i] = param_map.values[i] * oa_ratio_map.values[0].values[j] / capacity_to_activity_ratio * capacity_unit_factor * 8760
                                break
                    
                    target_db = ines_transform.add_item_to_DB(target_db, "flow_min_cumulative", [param["alternative_name"], (set_name,), "set"], param_map)
                    break #taking the flow from one of the outputs is enough

        for param in TotalTechnologyAnnualActivityUpperLimit:
            if param["entity_byname"] != unit_source["entity_byname"]:
                continue
            param_map = api.from_database(param["value"], param["type"])
            set_name = param["entity_byname"][1] + "_max_annual_activity"
            target_db = add_entity_and_entity_alternative(target_db, 'set', (set_name,), param["alternative_name"])
            
            for oa in oa_ratio:
                if oa["entity_byname"][0] == param["entity_byname"][0] and oa["entity_byname"][1] == param["entity_byname"][1]:
                    unit_name = oa["entity_byname"][0] + "__" + oa["entity_byname"][1]
                    node_name = oa["entity_byname"][0] + "__" + oa["entity_byname"][2]
                    ines_transform.assert_success(target_db.add_entity_item(entity_class_name='set__unit_flow', 
                                                                            entity_byname=(set_name, unit_name, node_name)), warn=True)
                    oa_ratio_map = api.from_database(oa["value"], oa["type"])
                    for i, val in enumerate(param_map.indexes):
                        for j, oa_val in enumerate(oa_ratio_map.values[0].indexes):
                            if val == oa_val:
                                param_map.values[i] = param_map.values[i] * oa_ratio_map.values[0].values[j] / capacity_to_activity_ratio * capacity_unit_factor * 8760
                                break
                    target_db = ines_transform.add_item_to_DB(target_db, "flow_max_cumulative", [param["alternative_name"], (set_name,), "set"], param_map)
                    break #taking the flow from one of the outputs is enough
        
        for param in TotalTechnologyModelPeriodActivityLowerLimit:
            if param["entity_byname"] != unit_source["entity_byname"]:
                continue
            param_float = api.from_database(param["value"], param["type"])
            set_name = param["entity_byname"][1] + "_min_model_activity"
            target_db = add_entity_and_entity_alternative(target_db, 'set', (set_name,), param["alternative_name"])
            
            for oa in oa_ratio:
                if oa["entity_byname"][0] == param["entity_byname"][0] and oa["entity_byname"][1] == param["entity_byname"][1]:
                    unit_name = oa["entity_byname"][0] + "__" + oa["entity_byname"][1]
                    node_name = oa["entity_byname"][0] + "__" + oa["entity_byname"][2]
                    ines_transform.assert_success(target_db.add_entity_item(entity_class_name='set__unit_flow', 
                                                                            entity_byname=(set_name, unit_name, node_name)), warn=True)
                    oa_ratio_map = api.from_database(oa["value"], oa["type"])
                    #taking the first value of the map, as INES supports only constant oa values
                    param_float = param_float * oa_ratio_map.values[0].values[0] / capacity_to_activity_ratio * capacity_unit_factor * 8760
                    target_db = ines_transform.add_item_to_DB(target_db, "flow_min_cumulative", [param["alternative_name"], (set_name,), "set"], param_float)
                    break #taking the flow from one of the outputs is enough
        
        for param in TotalTechnologyModelPeriodActivityUpperLimit:
            if param["entity_byname"] != unit_source["entity_byname"]:
                continue
            param_float = api.from_database(param["value"], param["type"])
            set_name = param["entity_byname"][1] + "_max_model_activity"
            target_db = add_entity_and_entity_alternative(target_db, 'set', (set_name,), param["alternative_name"])
            
            for oa in oa_ratio:
                if oa["entity_byname"][0] == param["entity_byname"][0] and oa["entity_byname"][1] == param["entity_byname"][1]:
                    unit_name = oa["entity_byname"][0] + "__" + oa["entity_byname"][1]
                    node_name = oa["entity_byname"][0] + "__" + oa["entity_byname"][2]
                    ines_transform.assert_success(target_db.add_entity_item(entity_class_name='set__unit_flow', 
                                                                            entity_byname=(set_name, unit_name, node_name)), warn=True)
                    oa_ratio_map = api.from_database(oa["value"], oa["type"])
                    #taking the first value of the map, as INES supports only constant oa values
                    param_float = param_float * oa_ratio_map.values[0].values[0] / capacity_to_activity_ratio * capacity_unit_factor * 8760
                    target_db = ines_transform.add_item_to_DB(target_db, "flow_max_cumulative", [param["alternative_name"], (set_name,), "set"], param_float)
                    break #taking the flow from one of the outputs is enough

    return target_db

def process_node_types(source_db, target_db):
    nodes = source_db.get_entity_items(entity_class_name="REGION__FUEL")
    alts = source_db.get_alternative_items()
    for node in nodes:
        for alt in alts:
            aa_demand = source_db.get_parameter_value_item(entity_class_name="REGION__FUEL",
                                                           entity_byname=node["entity_byname"],
                                                           alternative_name=alt["name"],
                                                           parameter_definition_name="AccumulatedAnnualDemand")
            sp_demand = source_db.get_parameter_value_item(entity_class_name="REGION__FUEL",
                                                           entity_byname=node["entity_byname"],
                                                           alternative_name=alt["name"],
                                                           parameter_definition_name="SpecifiedDemandProfile")
            if aa_demand and not sp_demand:
                p_value, p_type = api.to_database("balance_within_period")
                added, updated, error = target_db.add_update_parameter_value_item(entity_class_name="node",
                                                                                  entity_byname=(node["name"],),
                                                                                  alternative_name=alt["name"],
                                                                                  parameter_definition_name="node_type",
                                                                                  type=p_type,
                                                                                  value=p_value)
                if error:
                    exit("Failed to add node_type balance_within_period on nodes with AccumulatedAnnualDemand: " + error)
    try:
        target_db.commit_session("Added node_type balance_within_period to nodes with AccumulatedAnnualDemand")
    except:
        print("Failed to commit node_type balance_within_period on nodes with AccumulatedAnnualDemand")
    return target_db

##Check parameters:
#Done:
# Emission
# reMin
# activity constraints
# Performance -activity
# Costs
# Demand
# Storage - StorageMaxChargeRate, when two way technology. Consider the possibility of splitting that unit
# Capacity constraints

### Mode of operation transformation is still missing### 
### Reserves are not implemented ###


### Passes only one scenario, some parameters are not possible to transform for all alternatives

def alternative_name_from_two(alt_i, alt_o, target_db):
    if alt_i == alt_o:
        alt = alt_i
    else:
        alt = alt_i + "__" + alt_o
    target_db.add_update_alternative_item(name=alt)
    return alt


def get_parameter_values_with_default(source_db, source_entity_class, source_param, use_default = False, ignore_default_value_of = None, entity_byname = None, alternative_name = None):
    entities = source_db.get_entity_items(entity_class_name=source_entity_class) if use_default else None
    param_def_item = source_db.get_parameter_definition_item(
                        entity_class_name=source_entity_class, name=source_param
                        ) if use_default else None

    # Get all parameter values at once
    if alternative_name:
        params = source_db.get_parameter_value_items(
            entity_class_name=source_entity_class,
            parameter_definition_name=source_param,
            alternative_name=alternative_name
        )
    else:
        params = source_db.get_parameter_value_items(
            entity_class_name=source_entity_class,
            parameter_definition_name=source_param,
        )

    if use_default:
        if ignore_default_value_of != api.from_database(param_def_item["default_value"], param_def_item["default_type"]):
            entities_with_params = {tuple(p["entity_byname"]) for p in params}
            for entity in entities:
                if tuple(entity["entity_byname"]) not in entities_with_params:
                    params.append({
                        "entity_byname": entity["entity_byname"],
                        "value": param_def_item["default_value"],
                        "type": param_def_item["default_type"],
                        "alternative_name": default_alternative
                    })
    return params

def add_entity_and_entity_alternative(target_db, entity_class_name, entity_byname, alternative_name):
    ines_transform.assert_success(target_db.add_entity_item(entity_class_name = entity_class_name, entity_byname = entity_byname), warn=True)
    ines_transform.assert_success(target_db.add_update_entity_alternative_item(entity_class_name=entity_class_name, entity_byname=entity_byname,
                                                                               alternative_name=alternative_name, active=True), warn=True)
    return target_db

def round_to_nearest_minute(dt):
    new_seconds = (dt.second + 30) // 60 * 60  # Round seconds
    return dt + datetime.timedelta(seconds=new_seconds - dt.second)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        url_db_in = sys.argv[1]
    else:
        exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")
    if len(sys.argv) > 2:
        url_db_out = sys.argv[2]
    else:
        exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")
    with open('osemosys_to_ines_entities.yaml', 'r') as file:
        entities_to_copy = yaml.load(file, yaml.BaseLoader)
    with open('osemosys_to_ines_parameters.yaml', 'r') as file:
        parameter_transforms = yaml.load(file, yaml.BaseLoader)
    with open('osemosys_to_ines_methods.yaml', 'r') as file:
        parameter_methods = yaml.load(file, yaml.BaseLoader)
    with open('osemosys_to_ines_entities_to_parameters.yaml', 'r') as file:
        entities_to_parameters = yaml.load(file, yaml.BaseLoader)
    if len(sys.argv) > 3:
        settings_file = sys.argv[3]
        print(settings_file)
        with open(settings_file, 'r') as yaml_file:
            settings = yaml.safe_load(yaml_file)
    else:
         exit("Please provide the settings yaml file as the third argument.""")
    if len(sys.argv) > 4:
        timeslice_csv = sys.argv[4]
    else:
        exit("Please provide timeslices to time mapping csv file as the fourth argument.""")
    
    default_alternative = settings["default_alternative"]
    
    unlimited_unit_capacity = float(settings["unlimited_unit_capacity"])
    default_unit_size = float(settings["default_unit_size"])
    unit_to_penalty_boundary = float(settings["unit_to_penalty_boundary"])
    default_interest_rate = float(settings["default_interest_rate"])

    capacity_unit_factor = float(settings["capacity_unit_to_MW_factor"])
    storage_unit_factor = float(settings["storage_capacity_unit_to_MWh_factor"])
    demand_unit_factor = float(settings["demand_unit_to_MWh_factor"])
    investment_unit_factor = float(settings["investment_unit_to_CUR/MW_factor"])
    storage_investment_unit_factor = float(settings["storage_investment_unit_to_CUR/MWh_factor"])
    variable_cost_unit_factor = float(settings["variable_cost_unit_to_CUR/MW_factor"])
    
    main()

