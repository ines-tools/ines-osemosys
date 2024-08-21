import spinedb_api as api
from spinedb_api import DatabaseMapping
from spinedb_api.helpers import Asterisk
from ines_tools import ines_transform
import numpy as np
from sqlalchemy.exc import DBAPIError
import sys
from sys import exit
import yaml
import itertools
import datetime
from dateutil.relativedelta import relativedelta

if len(sys.argv) > 1:
    url_db_in = sys.argv[1]
else:
    url_db_in = "sqlite:///OSeMOSYS_db.sqlite"
if len(sys.argv) > 2:
    url_db_out = sys.argv[2]
else:
    url_db_out = "sqlite:///ines-spec.sqlite"

with open('osemosys_to_ines_entities.yaml', 'r') as file:
    entities_to_copy = yaml.load(file, yaml.BaseLoader)
with open('osemosys_to_ines_parameters.yaml', 'r') as file:
    parameter_transforms = yaml.load(file, yaml.BaseLoader)
with open('osemosys_to_ines_methods.yaml', 'r') as file:
    parameter_methods = yaml.load(file, yaml.BaseLoader)
with open('osemosys_to_ines_entities_to_parameters.yaml', 'r') as file:
    entities_to_parameters = yaml.load(file, yaml.BaseLoader)
with open('settings.yaml', 'r') as file:
    settings = yaml.safe_load(file)
unlimited_unit_capacity = float(settings["unlimited_unit_capacity"])
default_unit_size = float(settings["default_unit_size"])
unit_to_penalty_boundary = float(settings["unit_to_penalty_boundary"])

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
            #target_db.purge_items('parameter_definition')
            #target_db.purge_items('entity_class')
            #target_db.purge_items('parameter_value_list')
            target_db.refresh_session()
            target_db.commit_session("Purged stuff")
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

            ## Copy entites
            target_db = ines_transform.copy_entities(source_db, target_db, entities_to_copy)
            ## Create periods from yesr
            target_db = create_periods(source_db, target_db)
            ## Copy timeslice parameters (manual scripting)
            target_db = process_timeslice_data(source_db, target_db)
            ## Copy numeric parameters(source_db, target_db, parameter_transforms)
            target_db = ines_transform.transform_parameters_use_default(source_db, target_db, parameter_transforms,
                                                                        default_alternative="base", ts_to_map=True)
            ## Copy method parameters
            target_db = ines_transform.process_methods(source_db, target_db, parameter_methods)
            ## Copy entities to parameters
            target_db = ines_transform.copy_entities_to_parameters(source_db, target_db, entities_to_parameters)
            ## Copy capacity specific parameters (manual scripting)
            target_db = process_capacities(source_db, target_db, unit_capacity=default_unit_size)
            ## Special model level parameters
            target_db = process_model_level(source_db, target_db)
            ## Process units with zero investment cost
            target_db = process_zero_investment_cost(source_db, target_db)
            ## Assign node types
            target_db = process_node_types(source_db, target_db)

def create_periods(source_db, target_db):
    models = source_db.get_entity_items(entity_class_name="model")
    years = source_db.get_entity_items(entity_class_name="YEAR")
    for model in models:
        added, error = target_db.add_entity_item(entity_class_name="system", name=model["name"])
        if error:
            exit("Could not add system entity to ines-db: " + error)

        added, error = target_db.add_entity_item(entity_class_name="temporality", name=model["name"])
        if error:
            exit("Could not add temporality entity to ines-db: " + error)

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
                p_value, p_type = api.to_database(api.Duration(relativedelta(years=years_represented)))
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
        p_value, p_type = api.to_database(api.Duration(relativedelta(years=years_represented)))
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
            added_ea, update_ea, error = target_db.add_update_entity_alternative_item(entity_class_name="temporality",
                                                                    entity_byname=(model["name"],),
                                                                    alternative_name=ea["alternative_name"],
                                                                    active=ea["active"])
            if error:
                exit("Adding temporality entity_alternative failed. " + error)
    try:
        target_db.commit_session("Added periods from YEARs to ines_db")
    except DBAPIError as e:
        print("failed to add periods")
    return target_db


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
    time_durations = []
    previous_time_duration = timeslice_to_time_data.values[0].values[0]
    for time_object in timeslice_to_time_data.values:
        timeslice_indexes.append(time_object.indexes[0])
        time_durations.append(time_object.values[0])
        #if previous_time_duration != time_object.values[0]:
            #exit("Variable time resolution not suppported, please make a timeslice to datetime mapping with only one time resolution (use the lowest common denominator)")
    # Store the model time resolution in ines_db
    p_value, p_type = api.to_database(previous_time_duration)
    added, error = target_db.add_parameter_value_item(entity_class_name="temporality",
                                                      parameter_definition_name="resolution",
                                                      entity_byname=tuple([model_item["name"]]),
                                                      alternative_name=timeslices_to_time[0]["alternative_name"],
                                                      value=p_value,
                                                      type=p_type)
    if error:
        exit("Could not add resolution parameter to ines-db: " + error)

    timeline_map = api.TimeSeriesVariableResolution(datetime_indexes, time_durations, ignore_year=False, repeat=False, index_name="timestamp")
    p_value, p_type = api.to_database(timeline_map)
    added, error = target_db.add_parameter_value_item(entity_class_name="system",
                                                      parameter_definition_name="timeline",
                                                      entity_byname=tuple([model_item["name"]]),
                                                      alternative_name=timeslices_to_time[0]["alternative_name"],
                                                      value=p_value,
                                                      type=p_type)
    if error:
        exit("Could not add timeline parameter to ines-db: " + error)

    datetime_block_start = datetime_indexes[0]
    datetime_block_starts = []
    datetime_block_durations = []
    for k, datetime_index in enumerate(datetime_indexes[:-1]):
        if datetime_indexes[k + 1].value - datetime_index.value != datetime.timedelta(hours=time_durations[k]):
            datetime_block_starts.append(datetime_block_start)
            datetime_block_durations.append(api.Duration(relativedelta(datetime_index.value,
                                                                       datetime_block_start.value)
                                                         + relativedelta(hours=time_durations[k])))
            datetime_block_start = datetime_indexes[k + 1]
    spine_array = api.Array(values=datetime_block_starts,
                            #value_type=api.DateTime,
                            index_name="datetime")
    p_value, p_type = api.to_database(spine_array)
    added, error = target_db.add_parameter_value_item(entity_class_name="solve_pattern",
                                                      parameter_definition_name="start_time",
                                                      entity_byname=(model_item["name"],),
                                                      alternative_name=timeslices_to_time[0]["alternative_name"],
                                                      value=p_value,
                                                      type=p_type)
    if error:
        print("process timeblock starttimes error: " + error)
    spine_array = api.Array(values=datetime_block_durations,
                            index_name="duration")
    p_value, p_type = api.to_database(spine_array)
    added, error = target_db.add_parameter_value_item(entity_class_name="solve_pattern",
                                                      parameter_definition_name="duration",
                                                      entity_byname=(model_item["name"],),
                                                      alternative_name=timeslices_to_time[0]["alternative_name"],
                                                      value=p_value,
                                                      type=p_type)
    if error:
        print("process timeblock durations error: " + error)

    try:
        target_db.commit_session("Added starttimes and durations for timeslices")
    except DBAPIError as e:
        print("failed to add parameter values for starttimes and durations data. " + e)

    # Go through parameters that use time indexes
    for year_split in source_db.get_parameter_value_items(entity_class_name="model",
                                                          entity_name=model_item["name"],
                                                          parameter_definition_name="YearSplit"):
        year_split_data = api.from_database(year_split["value"], year_split["type"])
        target_db = add_timeslice_data(source_db, target_db, year_split_data, year_split["alternative_name"],
                                       "REGION__FUEL", "SpecifiedDemandProfile", "node", "flow_profile",
                                       timeslice_indexes, datetime_indexes, -1.0, True)
        target_db = add_timeslice_data(source_db, target_db, year_split_data, year_split["alternative_name"],
                                       "REGION__TECHNOLOGY", "CapacityFactor", "unit", "availability",
                                       timeslice_indexes, datetime_indexes, 1.0, False)
    return target_db


def add_timeslice_data(source_db, target_db, year_split_data, alternative_name, source_class_name,
                       source_param_name, target_class_name, target_param_name, timeslice_indexes, datetime_indexes,
                       multiplier, scale_with_time):
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
                if scale_with_time:
                    timeslice_profiles[profile_data.indexes[s]] = multiplier * \
                                                                  round(float(profile_data_by_slices.values[0]) /  # Note that this takes the first value from the array of years (first year)
                                                                        float(year_split_data.values[s].values[0]), 6)
                else:
                    timeslice_profiles[profile_data.indexes[s]] = multiplier * \
                                                                  round(float(profile_data_by_slices.values[0]), 6)  # Note that this takes the first value from the array of years (first year)
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
    except DBAPIError as e:
        print("failed to add parameter values for timeslice data")
        print(e)
    return target_db


def process_capacities(source_db, target_db, unit_capacity):
    region__tech__fuel_entities = source_db.get_entity_items(entity_class_name="REGION__TECHNOLOGY__FUEL")
    for unit_source in source_db.get_entity_items(entity_class_name="REGION__TECHNOLOGY"):

        # Store parameter existing_units (for the alternatives that define it)
        source_unit_residual_capacity = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="ResidualCapacity")
        for param in source_unit_residual_capacity:
            param_map = api.from_database(param["value"], "map")
            param_map.values = [x * 1000 / unit_capacity for x in param_map.values]
            alt_ent_class = (param["alternative_name"], (unit_source["name"],), "unit")
            target_db = ines_transform.add_item_to_DB(target_db, "existing_units", alt_ent_class, param_map)

        source_unit_investment_cost = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="CapitalCost")
        source_unit_fixed_cost = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="FixedCost")
        source_unit_variable_cost = source_db.get_parameter_value_items(entity_class_name="REGION__TECHNOLOGY", entity_name=unit_source["name"], parameter_definition_name="VariableCost")

        act_indexes = None
        input_act_ratio = []
        output_act_ratio = []
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
        if input_act_params:
            for param in input_act_params:
                mode_map_objects = api.from_database(param["value"], "map")
                for k, input_map_object in enumerate(mode_map_objects.values):
                    input_act_ratio.append({param["alternative_name"]: input_map_object.values})
                    if act_indexes:
                        if not act_indexes == input_map_object.indexes:
                            exit("InputActivityRatio and/or OutputActivityRatio contain inconsistent YEAR indexes for " and rtf_ent["name"])
                    act_indexes = input_map_object.indexes
        if output_act_params:
            for param in output_act_params:
                mode_map_objects = api.from_database(param["value"], "map")
                for k, output_map_object in enumerate(mode_map_objects.values):
                    output_act_ratio.append({param["alternative_name"]: output_map_object.values})
                    if act_indexes:
                        if not act_indexes == output_map_object.indexes:
                            exit("InputActivityRatio and/or OutputActivityRatio contain inconsistent YEAR indexes")
                    act_indexes = output_map_object.indexes

        if len(input_act_ratio) == 1 and len(output_act_ratio) == 1:
            for alt_i, iar in input_act_ratio[0].items():
                for alt_o, oar in output_act_ratio[0].items():
                    alt = alternative_name_from_two(alt_i, alt_o)
                    #output_map_object.values = [o / i for o, i in zip(oar, iar)]
                    alt_ent_class = (alt, (unit_source["name"],), "unit")
                    #target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, output_map_object)
                    target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, oar[0] / iar[0])
        elif len(input_act_ratio) == 0 and len(output_act_ratio) == 1:
            for alt_o, oar in output_act_ratio[0].items():
                alt_ent_class = (alt_o, (unit_source["name"],), "unit")
                #output_map_object.values = oar
                #target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, output_map_object)
                #target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, oar[0])
        elif len(input_act_ratio) == 1 and len(output_act_ratio) == 0:
            for alt_i, iar in input_act_ratio[0].items():
                alt_ent_class = (alt_i, (unit_source["name"],), "unit")
                #input_map_object.values = 1 / iar
                #target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, input_map_object)
                #target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, 1 / iar[0])
        elif len(input_act_ratio) == 0 and len(output_act_ratio) == 0:
            exit("No InputActivityRatio nor OutputActivityRatio defined for " + unit_source["name"])
        else:
            input_list = []
            output_list = []
            alt_set = set()
            alt_list = []
            for k, input_ar in enumerate(input_act_ratio):
                input_list.append({})
                for alt_i, iar in input_ar.items():
                    input_list[k][alt_i] = iar
                    alt_set.add(alt_i)
            for k, output_ar in enumerate(output_act_ratio):
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
                    #output_map_object.values = [round(o / i, 6) for o, i in zip(summed_output, summed_input)]
                    #output_map_object.index_name = "period"
                    alt_ent_class = (alt, (unit_source["name"],), "unit")
                    #target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, output_map_object)
                    target_db = ines_transform.add_item_to_DB(target_db, "efficiency", alt_ent_class, summed_output[0] / summed_input[0])
            if len(output_values) > 1:
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
                                alt = alternative_name_from_two(alt_1, alto)
                                alt_ent_class = (alt, ent_byname, "unit_flow__unit_flow")
                                target_db = ines_transform.add_item_to_DB(target_db, "equality_ratio", alt_ent_class, output_map_object)
                        output_1 = out[alto]
                        alt_1 = alto

        if len(output_act_ratio) == 1:
            act_ratio_dict = output_act_ratio[0]
            act_param = output_act_params[0]
            entity_byname = output_names[0]
            entity_byname = (entity_byname[0] + "__" + entity_byname[1], entity_byname[0] + "__" + entity_byname[2])
            class_name = "unit__to_node"
        elif len(input_act_ratio) == 1:
            act_ratio_dict = input_act_ratio[0]
            act_param = input_act_params[0]
            entity_byname = input_names[0]
            entity_byname = (entity_byname[0] + "__" + entity_byname[2], entity_byname[0] + "__" + entity_byname[1])
            class_name = "node__to_unit"
        else:
            exit("Not handling multiple inputs together with multiple outputs. Error in entity: " + unit_source["name"])

        for alt_activity, act_ratio in act_ratio_dict.items():
            source_param = api.from_database(act_param["value"], "map")
            source_param = source_param.values[0]  # Drop mode_of_operation dimension (assuming there is only one)
            if not np.all(x == source_param.values[0] for x in source_param.values):
                exit("The unit changes it's activity ratio between years - this is not handled. Entity: " + entity_byname)
            capacity_value = 1000 / source_param.values[0]
            alt_ent_class = (alt_activity, entity_byname, class_name)
            target_db = ines_transform.add_item_to_DB(target_db, "capacity", alt_ent_class, capacity_value)
            flag_allow_investments = False
            alt_inv_cost = alt_activity
            alt_fixed_cost = alt_activity
            for source_param in source_unit_investment_cost:
                alt = alternative_name_from_two(source_param["alternative_name"], alt_activity)
                source_param = api.from_database(source_param["value"], "map")
                source_param.values = [s / a for s, a in zip(source_param.values, act_ratio)]
                source_param.index_name = "period"
                alt_ent_class = (alt, entity_byname, class_name)
                target_db = ines_transform.add_item_to_DB(target_db, "investment_cost", alt_ent_class, source_param)
                if class_name == "unit__to_node":
                    unit_byname = (entity_byname[0],)
                if class_name == "node__to_unit":
                    unit_byname = (entity_byname[1],)
                if max(source_param.values) > 0 and min(source_param.values) == 0.0:
                    exit("Investment cost 0 for some years and above 0 for others - don't know how to handle")
                if min(source_param.values) > 0:
                    flag_allow_investments = True
                    alt_inv_cost = alt_activity
            for source_param in source_unit_fixed_cost:
                alt = alternative_name_from_two(source_param["alternative_name"], alt_activity)
                source_param = api.from_database(source_param["value"], "map")
                source_param.values = [s / a for s, a in zip(source_param.values, act_ratio)]
                source_param.index_name = "period"
                alt_ent_class = (alt, entity_byname, class_name)
                target_db = ines_transform.add_item_to_DB(target_db, "fixed_cost", alt_ent_class, source_param)
                if max(source_param.values) > 0 and min(source_param.values) == 0.0:
                    exit("Fixed cost 0 for some years and above 0 for others - don't know how to handle")
                if min(source_param.values) > 0:
                    flag_allow_investments = True
                    alt_fixed_cost = alt_activity
            if flag_allow_investments:
                p_value, p_type = api.to_database("no_limits")
            else:
                p_value, p_type = api.to_database("not_allowed")
            alt = alternative_name_from_two(alt_inv_cost, alt_fixed_cost)
            added, error = target_db.add_parameter_value_item(entity_class_name="unit",
                                                              entity_byname=unit_byname,
                                                              parameter_definition_name="investment_method",
                                                              alternative_name=alt,  # This is not really satisfactory, if there are values across different alternatives. Tries to do something, but it's shaky.
                                                              value=p_value,
                                                              type=p_type)
            if error:
                exit("error in trying to add investment_method: " + error)
            for source_param in source_unit_variable_cost:
                alt = alternative_name_from_two(source_param["alternative_name"], alt_activity)
                source_param = api.from_database(source_param["value"], "map")
                if len(source_param) > 1:
                    exit("More than one mode_of_operation with variable_cost defined. Can't handle that. Entity: " + entity_name)
                source_param = source_param.values[0]  # Bypass mode_of_operation dimension (assume there is only one)
                source_param.values = [s * 3.6 / a for s, a in zip(source_param.values, act_ratio)]
                source_param.index_name = "period"
                alt_ent_class = (alt, entity_byname, class_name)
                target_db = ines_transform.add_item_to_DB(target_db, "other_operational_cost", alt_ent_class, source_param)

        if (len(output_act_ratio) == 1 and len(input_act_ratio) == 0) or (len(output_act_ratio) == 0 and len(input_act_ratio) == 1):
            for alt_activity, act_ratio in act_ratio_dict.items():
                alt_ent_class = (alt_activity, (unit_source["name"],), "unit")
                target_db = ines_transform.add_item_to_DB(target_db, "conversion_method", alt_ent_class, "coefficients_only")
        if len(output_act_ratio) > 0 and len(input_act_ratio) > 0:
            for alt_activity, act_ratio in act_ratio_dict.items():
                alt_ent_class = (alt_activity, (unit_source["name"],), "unit")
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
            flag_invest_zero = False
            flag_fixed_zero = False
            flag_existing_zero = False
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
            if flag_invest_zero and flag_fixed_zero and flag_existing_zero:
                variable_cost = source_db.get_parameter_value_item(entity_class_name="REGION__TECHNOLOGY",
                                                                   entity_byname=unit["entity_byname"],
                                                                   alternative_name=alt["name"],
                                                                   parameter_definition_name="VariableCost")
                p_value, p_type = api.to_database(unlimited_unit_capacity / default_unit_size)
                added, updated, error = target_db.add_update_parameter_value_item(entity_class_name="unit",
                                                                                  entity_byname=(unit["name"],),
                                                                                  alternative_name=alt["name"],
                                                                                  parameter_definition_name="existing_units",
                                                                                  type=p_type,
                                                                                  value=p_value)
                if error:
                    exit("Failed to add existing capacity for a unit without investment cost or existing capacity: " + error)

                if not variable_cost:
                    print("Warning: unit " + unit["name"] + " does not have investment cost, existing capacity nor variable cost. Maybe not limited.")
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
                            # output_activity_ratio =
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

def alternative_name_from_two(alt_i, alt_o):
    if alt_i == alt_o:
        alt = alt_i
    else:
        alt = alt_i + "__" + alt_o
    return alt

if __name__ == "__main__":
    main()

