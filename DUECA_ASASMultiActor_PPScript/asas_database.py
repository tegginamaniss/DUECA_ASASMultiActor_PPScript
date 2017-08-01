import datetime
import glob
import pprint
import re
import warnings
from os.path import abspath, basename

import dataset
import sqlalchemy

pp = pprint.PrettyPrinter(indent=4)


class DataBase:
    def __init__(self, url: str):
        self.db = dataset.connect(url)
        self.ac_id_line = 4
        self.initialize_tables()

    def initialize_tables(self):
        draw_divider()
        if "log_files" not in self.db:
            log_files_table = self.db.create_table("log_files")
            log_files_table.create_column("log_file_name", sqlalchemy.String)
            log_files_table.create_column("log_date_time", sqlalchemy.DateTime)
            log_files_table.create_column("group_id", sqlalchemy.String)
            log_files_table.create_column("scenario_id", sqlalchemy.String)
        self.log_files_register = self.db['log_files']

        if "ac_params" not in self.db:
            ac_params_table = self.db.create_table('ac_params')

            # ac_params_table.create_column("group_id", sqlalchemy.String)

            ac_params_table.create_column("ac_id", sqlalchemy.String)
            ac_params_table.create_column("ac_ipos", sqlalchemy.String)

            ac_params_table.create_column("sim_time", sqlalchemy.String)
            ac_params_table.create_column("scenario", sqlalchemy.String)

            ac_params_table.create_column("posx", sqlalchemy.Float)
            ac_params_table.create_column("posy", sqlalchemy.Float)
            ac_params_table.create_column("posz", sqlalchemy.Float)
            ac_params_table.create_column("psi", sqlalchemy.Float)
            ac_params_table.create_column("tas", sqlalchemy.Float)
            ac_params_table.create_column("cas", sqlalchemy.Float)
            ac_params_table.create_column("sel_hdg", sqlalchemy.Float)
            ac_params_table.create_column("sel_spd", sqlalchemy.Float)
            ac_params_table.create_column("nd_range", sqlalchemy.Integer)
            ac_params_table.create_column("nd_mode", sqlalchemy.Integer)
        self.ac_parameters_table = self.db['ac_params']

    def add_log_file(self, file_path: str):
        file_name = basename(file_path)
        entries = ["posx", "posy", "posz", "psi", "tas", "cas", "sel_hdg", "sel_spd", "nd_range", "nd_mode"]

        if self.log_files_register.count(log_file_name=file_name) > 0:
            warnings.warn("Log file entry omitted due to repetition: " + file_name)
            return

        # Information on logged file
        group_id, scenario_id, date_time = self.parse_file_name(file_name)
        self.log_files_register.insert({
            "log_file_name": file_name,
            "log_date_time": date_time,
            "group_id": group_id,
            "scenario_id": scenario_id})

        # information in logged file
        with open(file_path, 'r') as f:
            f_data = f.readlines()[self.ac_id_line:]
            ac_id_list = f_data[0][2:-1].split(' ')
            ac_ipos_list = ['AC-' + str(number+1) for number in range(len(ac_id_list))]

            def parameter_table_entries(ac_props):
                sim_data = [item[:-1].split(', ') for item in f_data[2:]]
                for line in sim_data:
                    sim_time = line[0]
                    scenario_num = line[1]

                    for ac_id, ac_ipos, parameters in zip(ac_id_list, ac_ipos_list, chunks(line[2:], len(ac_props))):
                        yield {
                            "ac_id": str(ac_id),
                            "ac_ipos": str(ac_ipos),
                            "sim_time": float(sim_time),
                            "scenario": int(scenario_num),
                            "posx": float(parameters[0]),
                            "posy": float(parameters[1]),
                            "posz": float(parameters[2]),
                            "psi": float(parameters[3]),
                            "tas": float(parameters[4]),
                            "cas": float(parameters[5]),
                            "sel_hdg": float(parameters[6]),
                            "sel_spd": float(parameters[7]),
                            "nd_range": float(parameters[8]),
                            "nd_mode": float(parameters[9]),
                        }
            self.ac_parameters_table.insert_many(parameter_table_entries(entries))
        print("Successfully added:\t" + basename(file_path))

    def add_log_files_from_folder(self, folder_path: str):
        file_paths = glob.glob(abspath(folder_path + "/log-scenario_*.txt"))
        if len(file_paths):
            for file_path in file_paths:
                self.add_log_file(file_path)
            draw_divider()
        else:
            warnings.warn("No log files detected in folder: " + abspath(folder_path) + self.format_message())

    def parse_file_name(self, f_name: str):
        scenario_data = re.search("scenario_(\w+)", f_name).groups()[0]
        group_id = re.search("(G\d+)", scenario_data).group()
        scenario_id = re.search("_(\w+)", scenario_data).groups()[0]
        date_time = self.split_date(re.search("(2\d{11})", f_name).group())
        return group_id, scenario_id, date_time

    @staticmethod
    def format_message():
        msg0 = "\nThe format of the filename is not detected. The filename must be of the format:"
        msg1 = "\n\t\tlog-scenario_G<number>_<scenarioName>-<YYYYMMDDHHMM>.txt"
        msg2 = "\nWhere,\n\t\t<number>\t\t is the subject group number"
        msg3 = "\n\t\t<scenarioName>\t is the scenario name used in DUECA Simulation"
        msg4 = "\n\t\t<YYYYMMDDHHMM>\t is the date and time stamp of the log"
        msg5 = "\nExample:"
        msg6 = "\n\t\tlog-scenario_G6_C3I3-201708011716.txt"
        return msg0 + msg1 + msg2 + msg3 + msg4 + msg5 + msg6 + "\n"

    @staticmethod
    def split_date(stamp: str):
        return datetime.datetime(
            int(stamp[0:4]),
            int(stamp[4:6]),
            int(stamp[6:8]),
            int(stamp[8:10]),
            int(stamp[10:12]))

    def logged_files(self, detailed=False):
        if detailed:
            for data in self.log_files_register.all():
                yield data
        else:
            for data in self.log_files_register.all():
                yield data['log_file_name']

    def groups_logged(self, detailed=False):
        undetailed = list(set(self.__groups_data()))
        undetailed.sort()
        if detailed:
            for idx, group_id in enumerate(undetailed):
                scene_ids = []
                matches = list(self.log_files_register.find(group_id=group_id))
                [scene_ids.append(["Scenario '" + str(match['scenario_id']) + "' conducted on " + str(match['log_date_time'])]) for match in matches]
                result = {group_id: scene_ids}
                yield result
        else:
            result = {'Logged Groups': undetailed,
                      'Number of Logged Groups': len(undetailed)}
            yield result

    def __groups_data(self):
        for data in self.log_files_register.all():
            yield data['group_id']

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def draw_divider():
    divider = "=" * 100
    print(divider + "\n")

if __name__ == "__main__":
    e = DataBase('sqlite:///:memory:')
    e.add_log_files_from_folder("./logdata/")

    pp.pprint(list(e.groups_logged()))
    pp.pprint(list(e.logged_files()))


