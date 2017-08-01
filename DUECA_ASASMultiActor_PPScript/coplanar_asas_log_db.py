import glob
import os
import pprint
from os.path import basename

import dataset
import sqlalchemy

pp = pprint.PrettyPrinter(indent=4)


class DataBase:
    def __init__(self, url: str):
        self.database_url = url
        self.db = dataset.connect(url)
        self.initialize_tables()

    def initialize_tables(self):
        if "log_files" not in self.db:
            log_file_table = self.db.create_table("log_files", primary_id="log_files_idx")
            log_file_table.create_column("log_file_name", sqlalchemy.String)
            log_file_table.create_column("log_date_time", sqlalchemy.DateTime)

        self.log_files_table = self.db['log_files']
# regexr {regular expression}
        if "ac_parameters" not in self.db:
            ac_parameters_table = self.db.create_table("ac_parameters")
            ac_parameters_table.create_column("aircraft_id", sqlalchemy.String)
            ac_parameters_table.create_column("analysis_id", sqlalchemy.String)
            ac_parameters_table.create_column("scenario_id", sqlalchemy.String)

            ac_parameters_table.create_column("time", sqlalchemy.Float)
            ac_parameters_table.create_column("posx", sqlalchemy.Float)
            ac_parameters_table.create_column("posy", sqlalchemy.Float)
            ac_parameters_table.create_column("posz", sqlalchemy.Float)
            ac_parameters_table.create_column("psi", sqlalchemy.Float)
            ac_parameters_table.create_column("tas", sqlalchemy.Float)
            ac_parameters_table.create_column("cas", sqlalchemy.Float)
            ac_parameters_table.create_column("sel_hdg", sqlalchemy.Float)
            ac_parameters_table.create_column("sel_spd", sqlalchemy.Float)
            ac_parameters_table.create_column("nd_range", sqlalchemy.Integer)
            ac_parameters_table.create_column("nd_mode", sqlalchemy.Integer)
        self.ac_parameters_table = self.db['ac_parameters']

    def add_log_file(self, path: str):
        if self.log_files_table.count(log_file_name=basename(path)) == 0:
            self.log_files_table.insert({"log_file_name": basename(path)})

            with open(path, 'r') as f:
                f_data = list(f.readlines()[4::])

                acid_list = f_data[0][2:-1].split(' ')

                ac_list = ['AC-' + str(number+1) for number in range(len(acid_list))]

                def parameters_table_entries():
                    sim_data = [item[0:-1].split(', ') for item in f_data[2::]]
                    for line in sim_data:
                        time = line[0]
                        scenario_id = line[1]

                        for acid, ac, parameters in zip(acid_list, ac_list, chunks(line[2:], 10)):
                            yield {
                                "aircraft_id": acid,
                                "analysis_id": ac,
                                "scenario_id": scenario_id,
                                "time": float(time),
                                "posx": float(parameters[0]),
                                "posy": float(parameters[1]),
                                "posz": float(parameters[2]),
                                "psi": float(parameters[3]),
                                "tas": float(parameters[4]),
                                "cas": float(parameters[5]),
                                "sel_hdg": float(parameters[6]),
                                "sel_spd": float(parameters[7]),
                                "nd_range": int(parameters[8]),
                                "nd_mode": int(parameters[9])
                            }

                self.ac_parameters_table.insert_many(parameters_table_entries())

    def all_from_scenario_time(self, scenario_id, time):
        ac_params = {}
        for result in self.ac_parameters_table.distinct("analysis_id", scenario_id=scenario_id):
            ac_params[result['analysis_id']] = self.ac_parameters_table.find_one(
                analysis_id=result['analysis_id'],
                scenario_id=scenario_id,
                time=time
            )
        return ac_params

    def get_ac_parameters(self, scenario_id, analysis_id):
        ac_params = {
            "aircraft_id": None,
            "time": [],
            "posx": [],
            "posy": [],
            "posz": [],
            "psi": [],
            "tas": [],
            "cas": [],
            "sel_hdg": [],
            "sel_spd": [],
            "nd_range": None,
            "nd_mode": None,
        }
        for sample in self.ac_parameters_table.find(
                analysis_id=analysis_id,
                scenario_id=scenario_id,
                ):
            if ac_params["nd_range"] is None:
                ac_params["aircraft_id"] = sample["aircraft_id"]
                ac_params["nd_range"] = sample["nd_range"]
                ac_params["nd_mode"] = sample["nd_mode"]

            ac_params["time"].append(sample["time"])
            ac_params["posx"].append(sample["posx"])
            ac_params["posy"].append(sample["posy"])
            ac_params["posz"].append(sample["posz"])
            ac_params["psi"].append(sample["psi"])
            ac_params["tas"].append(sample["tas"])
            ac_params["cas"].append(sample["cas"])
            ac_params["sel_hdg"].append(sample["sel_hdg"])
            ac_params["sel_spd"].append(sample["sel_spd"])
        return ac_params

    def add_log_files_path(self, path: str):
        for filename in glob.glob(path + "/log-*.txt"):
            self.add_log_file(os.path.abspath(filename))

    def logged_files(self):
        for log_file_data in self.log_files_table.all():
            yield log_file_data['log_file_name']

    def aircraft_info(self, scenario_id):
        aircraft_info = {
            "ac_id": [],
            "analysis_id": [],
        }
        for result in self.ac_parameters_table.distinct("", scenario_id=scenario_id):
            print(self.ac_parameters_table.find_one(
                scenario_id=scenario_id
            ))



        # for sample in self.ac_parameters_table.find(scenario_id=scenario_id):
        #     aircraft_info["ac_id"].append(sample["aircraft_id"])
        #     aircraft_info["analysis_id"].append(sample["analysis_id"])
        # print(aircraft_info)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

db = DataBase("sqlite:///:memory:")
t = 5.0
db.add_log_files_path("./logdata/")
pp.pprint(db.all_from_scenario_time("1", t)['AC-5']['aircraft_id'])
t_ind = db.get_ac_parameters("1", "AC-2")['time'].index(t)
pp.pprint(db.get_ac_parameters("1", "AC-5")['aircraft_id'])

# pp.pprint(list(db.logged_files()))
# db.aircraft_info("1")
# pp.pprint(list(db.aircraft_info()))
