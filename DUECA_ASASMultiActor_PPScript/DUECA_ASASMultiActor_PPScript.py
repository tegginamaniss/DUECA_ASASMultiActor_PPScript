import matplotlib.patches as mpatch
import matplotlib.pyplot as plt
import numpy as np
import os, glob
import shapely.geometry as sg
from descartes import PolygonPatch

try:
    from DUECA_ASASMultiActor_PPScript.file import File as FileReader
except ModuleNotFoundError:
    from file import File as FileReader


def make_dict(key_list, value_list):
    res_dict = dict()
    for key, value in zip(key_list, value_list):
        res_dict[key] = value
    return res_dict


class DUECA_ASASMultiActor_PPScript:
    def __init__(self, input_folder=None, output_folder=None, show_plots=False, save_plots=None, formats=None):
        if input_folder is None:
            self.ip_folder = './logdata/'
        else:
            self.ip_folder = input_folder

        if output_folder is None:
            self.folder = os.path.abspath("./Output/")
        else:
            self.folder = os.path.abspath(output_folder)

        rel_path = self.ip_folder
        self.input_folder = os.path.abspath(rel_path)
        self.show_plots = show_plots
        if save_plots is None:
            self.save_plots = True
        elif isinstance(save_plots, bool):
            self.save_plots = save_plots
        else:
            raise Exception("paramter 'save_plots' must be a boolean")
        if formats is None:
            self.formats = ['svg']
        else:
            self.formats = formats
        self.data = None
        self.processed_data = []
        self.saved_names = []
        self.fig_width = None
        self.fig_height = None
        self.fig_num = None
        self.left = None
        self.bottom = None
        self.width = None
        self.height = None
        self.ov_num = None

        # Info about log file structure
        self.run_params = ['sim_time', 'scenario']
        self.ac_params = ['posx', 'posy', 'posz', 'psi', 'tas', 'cas', 'sel_hdg', 'sel_spd', 'nd_range', 'nd_mode']
        self.all_params = self.run_params
        for i2 in self.ac_params:
            self.all_params.append(i2)
        self.num_params = len(self.all_params)
        self.lines2omit = 6  # lines to omit in the beginning of the log file
        self.acid_line = 4  # line at which acids are present

        # Line thickness definitions
        self.markersize = 10
        self.vthin = 0.3
        self.thinline = 0.5
        self.line = 1
        self.thickline = 1.5

        # Begin the script
        self.__begin()

    def __begin(self):
        self.__read_input()

    def __read_input(self):
        for filename in glob.glob(self.input_folder + "/log-*.txt"):
            fread_object = open(filename, 'r')
            f_data = list(fread_object.readlines()[4::])

            acid_list = f_data[0][2:-1].split(' ')

            sim_data = [item[0:-1].split(', ') for item in f_data[2::]]
            self.split_data(acid_list, sim_data)
            # data_time_indexed, data_acid_indexed = self.split_data(acid_list, ac_data)
            # print(data_time_indexed)
            # print(data_time_indexed['0.5']['MS841'])

    def split_data(self, acid_list, sim_data):
        # print(self.get_data_time(acid_list, sim_data))
        self.get_data_acid(acid_list, sim_data)
        # return self.get_data_time(acid_list, sim_data), self.get_data_acid(acid_list, sim_data)

    def get_data_acid(self, acids, data):
        data_acid_dict = dict()
        param_dict = dict()
        n_ac = len(acids)

        data_transpose = list(map(list, zip(*data)))

        sim_time = data_transpose[0]
        scenario = data_transpose[1]
        ac_data = data_transpose[2::]
        inter_list2 = []

        for idx, ldata in enumerate(ac_data):
            print(acids[int(idx/10)], int(idx/10), self.ac_params[idx%10], idx%10, make_dict(sim_time, ldata))
            inter_list2.append(make_dict(sim_time, ldata))
            if not int(idx/10):
                print("\nidx")


        print()
        print(len(inter_list2))

        # sim_time = data_transpose[0]
        # scenario = data_transpose[1]
        # all_ac_data = data_transpose[2::]
        #
        # for idx, ldata in enumerate(all_ac_data):
        #     acid = acids[int(idx/len(self.ac_params))]
        #     param = self.ac_params[int(idx % len(self.ac_params))]
        #     if not int(idx % len(self.ac_params)):
        #         param_dict['sim_time'] = sim_time
        #     param_dict[param] = ldata
        #     if int(idx % len(self.ac_params)) == 9:
        #         data_acid_dict[acid] = param_dict
        # return data_acid_dict

    def get_data_time(self, acid_list, data):
        inter_list1 = []
        sim_time = []
        scenario = []
        for idx, ldata in enumerate(data):
            sim_time.append(ldata[0])
            scenario.append(ldata[1])
            ac_data = ldata[2::]

            inter_list2 = []

            for idx2, acid in enumerate(acid_list):
                b_idx = idx2*len(self.ac_params)
                e_idx = idx2*len(self.ac_params) + len(self.ac_params)
                acid_data = ac_data[b_idx:e_idx]
                inter_list2.append(make_dict(self.ac_params, acid_data))

            inter_list1.append(make_dict(acid_list, inter_list2))
        return make_dict(sim_time, inter_list1)



if __name__ == "__main__":
    view = DUECA_ASASMultiActor_PPScript(input_folder='./logdata/', formats=['svg', 'png'])































