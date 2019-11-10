"""Supporting functions to get QSTS metrics, reduce simulation time, and
reduce the RAM and storage requirements."""

import logging
import math
import random
import re
import sys

import matplotlib.pyplot as plt
import numpy as np
import opendssdirect as dss
import pandas as pd


logger = logging.getLogger(__name__)


def get_object_info(obj_class):
    """
    Get object information.
    """
    dss.Circuit.SetActiveClass(obj_class)
    flag = dss.ActiveClass.First()
    line = []

    while flag > 0:
        line.append({})
        line[-1]["name"]    = dss.CktElement.Name()
        line[-1]["limit"]   = dss.CktElement.NormalAmps()
        line[-1]["bus"]     = dss.CktElement.BusNames()
        line[-1]["current"] = np.array([abs(complex(i[0], i[1]))
                                        for i in zip(*[iter(dss.CktElement.Currents())]*2)])
        line[-1]["loading"] = max(line[-1]["current"] / line[-1]["limit"] * 100)
        flag = dss.ActiveClass.Next()

    return line


def get_time_window(tfinal,tinitial,time_vec, weight, stepsize):
    """Get time window."""
    time_window_last100 = []
    if len(time_vec) < 100: # Could increase or decrease this based on number of time points to consider in the window
        time_last100 = time_vec
    else:
        time_last100 = time_vec[-100:]

    for elem in time_last100:
        if elem <= tfinal and elem >= (tinitial - weight*stepsize):
            time_window_last100.append(elem)

    return time_window_last100

def get_index(time_vec, time_window_continuous, ind):
    """Get index."""
    if len(time_vec) < 100: # Could increase or decrease this based on number of time points to consider in the window
        time_last100 = time_vec
        index_add = 0
    else:
        time_last100 = time_vec[-100:]
        index_add = len(time_vec) - 100

    for elm in time_last100:
        if elm == time_window_continuous[ind]:
            index_last100 = time_last100.index(elm) + index_add

    return index_last100

def voltage_10min_metric(time_vec,ss,volt_mag):
    """
    Function to generate a 10 minute moving average of all bus voltages and
    track any buses whose 10 minute average voltage is outside the ANSI range.
    """
    t_present_index = len(time_vec)
    t_final = time_vec[t_present_index-1]
    t_win = 5
    volt_window = np.array([])
    volt_10min_avg = np.array([])
    volt_window_temp = list()

    if t_final < t_win:
        t_initial = time_vec[0]
    elif t_final >= t_win:
        t_ini_approx = t_final - t_win
        t_initial = max(get_time_window(t_ini_approx, t_ini_approx, time_vec, 11, ss))

    time_window_continuous = get_time_window(t_final, t_initial, time_vec, 0, ss)

    for ind in range(len(time_window_continuous)):
        index = (get_index(time_vec, time_window_continuous, ind))
        volt_window_temp.append(volt_mag.array[index - volt_mag.index])

    volt_window = np.array(volt_window_temp)
    volt_10min_avg = np.sum(volt_window, axis=0)/(len(volt_window))
    volt_10min_avg_max = np.amax(volt_10min_avg, axis=0)
    volt_10min_avg_min = np.amin(volt_10min_avg, axis=0)

    if (volt_10min_avg_max > 1.05 or volt_10min_avg_min < 0.95):
        tout = t_final
    else:
        tout = 0

    return tout, volt_10min_avg_max, volt_10min_avg_min

def loading_4hr_metric(time_vec,ss,loading_matrix,component):
    """
    Window for line and xfmr.
    """
    t_present_index = len(time_vec)
    t_final = time_vec[t_present_index-1]
    if component=='line':
        t_win = 1*4
    else:
        t_win = 2*4
    volt_window = np.array([])
    volt_2hr_avg = np.array([])
    volt_window_temp = list()

    if t_final < t_win:
        t_initial = time_vec[0]
    elif t_final >= t_win:
        t_ini_approx = t_final - t_win
        t_initial = max(get_time_window(t_ini_approx, t_ini_approx, time_vec, 5, ss))

    time_window_continuous = get_time_window(t_final, t_initial, time_vec, 0, ss)


    for ind in range(len(time_window_continuous)):
        index = (get_index(time_vec, time_window_continuous, ind))
        volt_window_temp.append(loading_matrix.array[index - loading_matrix.index])

    volt_window = np.array(volt_window_temp)
    volt_2hr_avg = np.sum(volt_window, axis=0)/(len(volt_window))
    volt_2hr_avg_max = np.amax(volt_2hr_avg, axis=0)

    if component=='line':
        if volt_2hr_avg_max > 100:
            tout = t_final
        else:
            tout = 0
    else:
        if volt_2hr_avg_max > 120:
            tout = t_final
        else:
            tout = 0

    return tout,volt_2hr_avg_max

#This can be improved upon by just sending the min and max values and no dicts
def get_volt_mag():
    """
    Get voltage magnitudes.
    """
    value = max(dss.Circuit.AllBusMagPu())
    voltage_max = value
    value = min(dss.Circuit.AllBusMagPu())
    voltage_min = value
    return voltage_max, voltage_min

def get_line():
    """
    Returns line loading (% of rated current capacity) of each line as a row vector.
    """
    dss.Circuit.SetActiveClass("Line")
    flag = dss.ActiveClass.First()
    Line1 = np.empty(dss.ActiveClass.Count()+1)
    Line1[0] = 0
    while flag >0:
        Line_current = []
        Line_limit = dss.CktElement.NormalAmps()
        phase = int((len(dss.CktElement.Currents())/4.0)%4)
        for ii in range(phase):
            Line_current.append(math.sqrt(dss.CktElement.Currents()[2*(ii)]**2+
                                          dss.CktElement.Currents()[2*ii+1]**2)
                                )
        Line1[flag] = max(Line_current)/Line_limit*100
        flag = dss.ActiveClass.Next()
    max_line_loading = max(Line1)
    return max_line_loading, Line1

def get_transformer():
    """
    Returns transformer loading (% of rated current capacity) of each transformer as a row vector.
    """
    dss.Circuit.SetActiveClass("Transformer")
    flag = dss.ActiveClass.First()
    Transformer1 = np.empty(dss.ActiveClass.Count()+1)
    Transformer1[0] = 0

    while flag > 0:
        Transformer_current = []
        Transformer_limit = dss.CktElement.NormalAmps()
        if dss.CktElement.NumPhases() == 3:
            phase = 4
        else:
            phase = int((len(dss.CktElement.Currents())/4.0)%4)
        for ii in range(phase):
            Transformer_current.append(math.sqrt(dss.CktElement.Currents()[2*ii]**2+
                                                 dss.CktElement.Currents()[2*ii+1]**2)
                                       )
        Transformer1[flag] = max(Transformer_current)/Transformer_limit*100
        flag = dss.ActiveClass.Next()

    max_transformer_loading = max(Transformer1)

    return max_transformer_loading,Transformer1

def Get_time_violation(time_window_largePV):
    """Get time violation."""
    loopcount=1
    temp_count = 0
    time_counter_array = []
    total_10min_avg_violation = 0
    for time_entry in time_window_largePV:

        #print(time_entry)
        if time_entry != 0:
            time_counter_array.append(time_entry)
            temp_count = temp_count + 1

        if time_entry == 0 or loopcount == len(time_window_largePV):
            if len(time_counter_array) > 1 and loopcount != len(time_window_largePV):
                total_10min_avg_violation = total_10min_avg_violation + (time_counter_array[len(time_counter_array)-1] - time_counter_array[0])

            if len(time_counter_array) > 1 and loopcount == len(time_window_largePV):
                total_10min_avg_violation = total_10min_avg_violation + (time_counter_array[len(time_counter_array)-1] - time_counter_array[0])

            temp_count = 0
            time_counter_array = []
        loopcount += 1
    return total_10min_avg_violation

def get_control_dev_ops(path):
    """TODO"""
    Event_Log = pd.read_csv(path, usecols=[0, 1, 2, 3, 4],
                            names=["Hour", "Seconds", "ControlIter", "Element", "Action"])

    # Get unique control devices
    control_devices = list()
    for cd in Event_Log["Element"]:
        cd = re.findall(r'Element=([A-Z].*)', cd)
        if cd not in control_devices:
            control_devices.append(cd)

    # Separate control devices into capacitors and Regulators
    Regulator = list()
    Capacitor = list()
    for cd in control_devices:
        cds = re.search(r'(?=.)\w+', cd[0])
        if cds.group(0) == 'Regulator':
            Regulator.append(cd)
        elif cds.group(0) == 'Capacitor':
            Capacitor.append(cd)

    # Get state changes of capacitors
    Cap_states = dict()
    for cap in Capacitor:
        count = 0
        Cap_state_changes = list()
        for cd in Event_Log["Element"]:
            if re.findall(r'Element=([A-Z].*)', cd) == cap:
                action = Event_Log.iloc[count]['Action']
                state = re.findall(r'Action=..([A-Z].*)..', action)
                if (state[0] == 'OPENED' or state[0] == 'CLOSED' or state[0] == 'STEP UP'):
                    Cap_state_changes.append(state)
            count = count + 1
        if cap[0] not in Cap_states:
            Cap_states[cap[0]] = len(Cap_state_changes)

    # Get tap changes of Regulators
    Reg_taps = dict()
    for reg in Regulator:
        count = 0
        total_taps = 0
        for cd in Event_Log["Element"]:
            if re.findall(r'Element=([A-Z].*)', cd) == reg:
                action = Event_Log.iloc[count]['Action']
                tap = re.findall(r'Action= CHANGED\s(\S.*)\sTAP', action)
                tap = abs(int(tap[0]))
                total_taps += tap
            count = count + 1
        if reg[0] not in Reg_taps:
            Reg_taps[reg[0]] = total_taps

    return Cap_states, Reg_taps

def get_inst_violations(param_list, threshold, comparator):
    """
    Get the number of instantaneous violations.
    """
    number_violations = 0
    for elem in param_list:
        if comparator == '>':
            if elem > threshold:
                number_violations+=1
        elif comparator == '<':
            if elem < threshold:
                number_violations+=1
    return number_violations

def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


def rand_pdf(PDF_X, PDF_Y, **kwargs):
    """
         TODO
     """
    if "verbose" in kwargs:
        verbose = kwargs["verbose"]
    else:
        verbose = False

    # Find the CDF associated with this PDF
    CDF_Y = np.cumsum(PDF_Y)

    # Randomly select a y between 0 and 1
    y = random.random() * 0.99999

    # Find the maximum index such that CDF_Y(index) < y
    x_index = np.argwhere(y < CDF_Y).flatten()
    if not x_index:
        logger.error("rand_pdf: y = {y}, x_index = {x}".format(y=y, x=x_index))
        logger.info(CDF_Y)

    # Find the x associated with that index.
    x = PDF_X[x_index[0]]

    if verbose:
        plt.plot(PDF_X, CDF_Y)

    return x

def GetFirstToken(LongDSSObjectName):
    """
    Define the GetFirstToken helper function, which extracts the first token name from a long DSS object name
    """
    #DOES NOT WORK.... :(
    #busnametokens = re.search(LongDSSObjectName, '^([^\.\s]*)([\.\s]|$)')
    #ShortDSSObjectName = busnametokens.group(0)
    ShortDSSObjectName = LongDSSObjectName.split(".")[0]
    return ShortDSSObjectName

def GetGraphMatricesfromOpenDSSCircuit(verbose):
    """
    TODO.
    """
    Arbitrarily_small_length_assigned_to_transformers = 0.01
    Arbitrarily_small_impedance_assigned_to_transformers = 0.01 + 0.02j

    #Extract the list of bus names and the number of buses
    AllBusNames = np.array(dss.Circuit.AllBusNames())
    NumBuses = len(AllBusNames)

    #Initialize the graph adjacency matrices G, G_Impedance, and G_Electrical_Distance
    DSSCircuit_GraphMatrices = {}

    DSSCircuit_GraphMatrices["G"]                     = np.zeros((NumBuses,NumBuses))
    DSSCircuit_GraphMatrices["G_Impedance"]           = np.zeros((NumBuses,NumBuses), dtype=np.complex)
    DSSCircuit_GraphMatrices["G_Electrical_Distance"] = np.zeros((NumBuses,NumBuses))

    NumPDE = dss.Lines.Count() + dss.Transformers.Count()

    DSSCircuit_GraphMatrices["Bus_to_Line_Incidence"] = np.zeros((NumPDE,NumBuses))
    DSSCircuit_GraphMatrices["Element_Names"]         = ['' for _ in range(NumPDE)]

    #Select the first line in the circuit
    Current_Element = dss.Circuit.FirstPDElement()

    while Current_Element>0:
        Current_Element -= 1
        #Extract the name and type of this element.
        ElementName = dss.CktElement.Name()
        DSSCircuit_GraphMatrices["Element_Names"][Current_Element] = ElementName

        if verbose:
            print("GetGraphMatricesfromOpenDSSCircuit: Current Element Name = < {} >".format(ElementName))

        ElementType = GetFirstToken(ElementName)

        #If this isn't a line or a transformer then skip it
        if ElementType == "Line" or ElementType == "Transformer":

            #Find the short bus names for the two buses attached to the current element (line or transformer)
            ElementBusNames = dss.CktElement.BusNames()
            Bus1_ShortName = GetFirstToken(ElementBusNames[0])
            Bus2_ShortName = GetFirstToken(ElementBusNames[1])

            #Find the bus indices associated with the two buses
            Bus1_Index = np.argwhere(
                            AllBusNames == Bus1_ShortName
                                ).flatten()

            if len(Bus1_Index) != 1:
                raise ValueError("GetGraphMatricesfromOpenDSSCircuit: Element #{cur} < {name} >: Bus1 < {b1name} > does not have a unique matching index.".format(
                                          cur=Current_Element, name=ElementName, b1name=Bus1_ShortName)
                )

            Bus2_Index = np.argwhere(
                            AllBusNames == Bus2_ShortName
                                ).flatten()

            if len(Bus2_Index) != 1:
                raise ValueError("GetGraphMatricesfromOpenDSSCircuit: Element #{cur} < {name} >: Bus2 < {b2name} > does not have a unique matching index.".format(
                                          cur=Current_Element, name=ElementName, b2name=Bus2_ShortName)
                )

            if verbose:
                print("GetGraphMatricesfromOpenDSSCircuit: Element < {name} > couples buses < {b1name} > and < {b2name} >".format(
                                 name=ElementName, b1name=Bus1_ShortName, b2name=Bus2_ShortName)
                )

            #Create the row in the incidence matrix associated with this element
            DSSCircuit_GraphMatrices["Bus_to_Line_Incidence"][Current_Element,:] = np.zeros((1, NumBuses))
            DSSCircuit_GraphMatrices["Bus_to_Line_Incidence"][Current_Element, Bus1_Index] = 1
            DSSCircuit_GraphMatrices["Bus_to_Line_Incidence"][Current_Element, Bus2_Index] = -1

            #Extract the length and impedance parameters of the element
            if ElementType == "Line":
                Element_Length = dss.Lines.Length()/1000.0
                Element_Impedance = complex(dss.Lines.R1(),
                                            dss.Lines.X1()
                                            )

            elif ElementType == "Transformer":
                Element_Length = Arbitrarily_small_length_assigned_to_transformers
                Element_Impedance = Arbitrarily_small_impedance_assigned_to_transformers

            else:
                raise ValueError("GetGraphMatricesfromOpenDSSCircuit: Unrecognized element type < {} >.".format(ElementType))

            if DSSCircuit_GraphMatrices["G"][Bus1_Index, Bus2_Index] == 0:
                #If bus1 and bus2 aren't already marked as adjacent, then set the
                #element of DSSCircuit_GraphMatrices.G at the intersection of the bus1 and bus2
                #indices to 1 to indicate they are adjacent.
                DSSCircuit_GraphMatrices["G"][Bus1_Index, Bus2_Index] = 1
                DSSCircuit_GraphMatrices["G"][Bus2_Index, Bus1_Index] = 1

                #Set the corresponding element of DSSCircuit_GraphMatrices.G_Impedance to the line impedance
                DSSCircuit_GraphMatrices["G_Impedance"][Bus1_Index, Bus2_Index] = Element_Impedance
                DSSCircuit_GraphMatrices["G_Impedance"][Bus2_Index, Bus1_Index] = Element_Impedance

                #Set the the corresponding element of DSSCircuit_GraphMatrices.G_Electrical_Distance to the line length
                DSSCircuit_GraphMatrices["G_Electrical_Distance"][Bus1_Index, Bus2_Index] = Element_Length
                DSSCircuit_GraphMatrices["G_Electrical_Distance"][Bus2_Index, Bus1_Index] = Element_Length
            else:
                #If Bus1 and Bus2 are already adjacent (that is, more than one
                #line connects them), then inverse-add the line impedances and
                #take the minimum line distance.
                DSSCircuit_GraphMatrices["G_Impedance"][Bus1_Index, Bus2_Index] = 1.0 / (
                                    1.0 / DSSCircuit_GraphMatrices["G_Impedance"][Bus1_Index, Bus2_Index] +
                                    1.0 / Element_Impedance)
                DSSCircuit_GraphMatrices["G_Impedance"][Bus2_Index, Bus1_Index] = 1.0 / (
                                    1.0 / DSSCircuit_GraphMatrices["G_Impedance"][Bus2_Index, Bus1_Index] +
                                    1.0 / Element_Impedance)
                DSSCircuit_GraphMatrices["G_Electrical_Distance"][Bus1_Index, Bus2_Index] = min(
                        DSSCircuit_GraphMatrices["G_Electrical_Distance"][Bus1_Index, Bus2_Index],
                        Element_Length)
                DSSCircuit_GraphMatrices["G_Electrical_Distance"][Bus2_Index, Bus1_Index] = min(
                        DSSCircuit_GraphMatrices["G_Electrical_Distance"][Bus2_Index, Bus1_Index],
                        Element_Length)

        #Select the next element in the circuit
        Current_Element = dss.Circuit.NextPDElement()

    return DSSCircuit_GraphMatrices

def get_data_at_idx(data_structure, index_array):
    """
    TODO  
    """
    new_data_structure = {}
    for key,value in data_structure.items():
        new_data_structure[key] = np.array(value)[index_array]
    return new_data_structure


def ismember(a_vec, b_vec):
    """ MATLAB equivalent ismember function """

    bool_ind = np.isin(a_vec,b_vec)
    common = a_vec[bool_ind]
    common_unique, common_inv = np.unique(common, return_inverse=True)
    b_unique, b_ind = np.unique(b_vec, return_index=True)
    common_ind = b_ind[np.isin(b_unique, common_unique, assume_unique=True)]
    return bool_ind, common_ind[common_inv]
