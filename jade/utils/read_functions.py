"""Utility functions for reading simulation inputs."""

import json
import pandas as pd
import numpy as np
import opendssdirect as dss
from jade.utils.simulation_utils import GetGraphMatricesfromOpenDSSCircuit


def Read_Init_File(DPV_Init_XLS_File_Name, _, verbose):
    """
    This function reads the DPV DPVInitizationFile.xls input.

    Parameters
    ----------
    DPV_Init_XLS_File_Name : str
        A string representing the path to the DPVInitizationFile.xls DPV input file.

    Returns
    -------
    dict
        Dictionary representing the parameters defined in the DPVInitizationFile.xls DPV input file.
        It contains the following fields:
        - Num_PV_Scenarios: Number of PV scenarios to generate.
        - Max_PV_Penetration: Maximum customer penetration of each PV scenario.
        - PV_Load_Scale_Factor: Load scale factor for DPV.
        - PV_Penetration_Values: Double array of PV penetration values for each PV scenario.
        - Num_PV_Deployments_per_Scenario: The length of PV_Penetration_Values.
    """
    if verbose:
        print("Read_DPV_Init_File: Reading DPV init data from {}.".format(DPV_Init_XLS_File_Name))

    #Open the JSON input file and store the input data in a dictionary
    with open(DPV_Init_XLS_File_Name, "r") as fp:
        init_data = json.load(fp)

    #Create the DPV_Init_File dict
    DPV_Init_File = {}

    #Num_PV_Scenarios
    DPV_Init_File["Num_PV_Scenarios"] = init_data["GenPVCases"]["Number of Scenarios"]

    #maximum PV customer penetration (e.g. num_PV_customers/num_total_customers * 100)
    DPV_Init_File["Max_PV_Penetration"] = init_data["GenPVCases"]["Maximum PV Penetration"]

    #PV/Load Scale Factor, which is a scaling applied to the kW size of PV installations
    DPV_Init_File["Max_PV_Penetration_LS"] = init_data["GenPVCases"]["Maximum PV Penetration"]

    DPV_Init_File["Num_PV_Scenarios_LS"] = init_data["GenPVCases"]["Number of Scenarios LS"]

    DPV_Init_File["PV_Load_Scale_Factor"] = init_data["GenPVCases"]["PV/load Scale Factor"]

    DPV_Init_File["feederLL"] = (init_data["GenPVCases"]["feederLL"] == 1)

    #load level of each scenario
    Tests_to_Run_Num = init_data["BookEnds"]["Load Level to Analyze"]

    DPV_Init_File["Run_Peak"]         = (1 in Tests_to_Run_Num)
    DPV_Init_File["Run_Offpeak"]      = (2 in Tests_to_Run_Num)
    DPV_Init_File["Run_Solarpeak"]    = (3 in Tests_to_Run_Num)
    DPV_Init_File["Run_Solaroffpeak"] = (4 in Tests_to_Run_Num)

    DPV_Init_File["Peak_Load_Scaling"] =  init_data["BookEnds"]["Absolute Maximum Load"]

    DPV_Init_File["Offpeak_Load_Scaling"] = init_data["BookEnds"]["Absolute Minimum Load"]

    DPV_Init_File["Solar_Peak_Load_Scaling"] = init_data["BookEnds"]["Solar Maximum Load"]

    DPV_Init_File["Solar_Offpeak_Load_Scaling"] = init_data["BookEnds"]["Solar Minimum Load"]

    #Read Voltage dividing primary with secondary/transmission
    DPV_Init_File["divide"] = [init_data["DPV_GUI"]["Voltage dividing Primary and Secondaries"],
                               init_data["DPV_GUI"]["Voltage dividing Primary and Transmission"]
                              ]

    #Read Threshold Values/Limits
    DPV_Init_File["Limits"] = {
        "Primary OverVoltage Threshold (PU)": init_data["DPV_GUI"]["Primary OverVoltage Threshold (PU)"],
        "Secondary OverVoltage (PU)": init_data["DPV_GUI"]["Secondary OverVoltage (PU)"],
        "Primary Deviation (PU)": init_data["DPV_GUI"]["Primary Deviation (PU)"],
        "Secondary Deviation (PU)": init_data["DPV_GUI"]["Secondary Deviation (PU)"],
        "Primary Imbalance (%)": init_data["DPV_GUI"]["Primary Imbalance (%)"],
        "Thermal (%)": init_data["DPV_GUI"]["Thermal (%)"],
        "Primary UnderVoltage Threshold (PU)": init_data["DPV_GUI"]["Primary UnderVoltage Threshold (PU)"],
        "Secondary UnderVoltage (PU)": init_data["DPV_GUI"]["Secondary UnderVoltage (PU)"],
        "Capacitor overvoltage threshold": init_data["DPV_GUI"]["Capacitor overvoltage threshold"],
        "Regulator deviation threshold": init_data["DPV_GUI"]["Regulator deviation threshold"],
        }

    #Read Regulator, Capacitor and LDC Bus Names
    DPV_Init_File["Reg_Buses"] = init_data["DPV_GUI"]["Regulator bus names"]

    DPV_Init_File["LDC_Buses"] = init_data["DPV_GUI"]["LDC information"]

    DPV_Init_File["Cap_Buses"] = init_data["DPV_GUI"]["Capacitor bus names"]

    return DPV_Init_File

def Read_Customer_Data(Customer_Data_XLS_Filename, verbose):
    """
    This function reads the DPV cust_data.xls input file into a Matlab Struct array representing the customer data.

    Parameters
    ----------
    Customer_Data_XLS_Filename : str
        A string representing the path to the cust_data.xls DPV input file.

    Returns
    -------
    dict
        Dictionary representing the customer data.
        Each customer Struct contains the following fields:
        - Bus_Name_Fully_Qualified: A string representing the DSS fully qualified (secondary) bus name
        - to which the customer is connected.
        - Type: A string representing the customer type. One of 'residential' or 'commercial'
        - Phase: The customer phase connection. One of 'A', 'B', 'C', or 'ABC'.
        - Bus_Name_Short: A string representing the OpenDSS short (secondary) bus name.
        - Primary_Bus_Name_Short: A string representing the OpenDSS short primary bus name.
        - Peak_Load: A double representing the customer's peak load.
        - Bus_Index: Integer representing the OpenDSS bus index of the customer's (secondary) bus.
        - Primary_Bus_Index: Integer representing the OpenDSS bus index of the customer's (primary) bus.
        - x: Double represnting customer's OpenDSS x location.
        - y: Double represnting customer's OpenDSS y location.
        - Distance: Double represnting customer's OpenDSS distance from feeder.

    """
    Progess_Update_Every_X_Customers = 100

    if verbose:
        print("Read_DPV_Customer_Data: Reading customer data from {}".format(Customer_Data_XLS_Filename))

    df = pd.read_excel(Customer_Data_XLS_Filename, sheet_name='bus_list', header=None)
    df2 = pd.read_excel(Customer_Data_XLS_Filename, sheet_name='primary_bus_list', header=None)
    df3 = pd.read_excel(Customer_Data_XLS_Filename, sheet_name='cust_type', header=None)
    df4 = pd.read_excel(Customer_Data_XLS_Filename, sheet_name='cust_phase', header=None)

    Customer_PeakLoad = pd.read_excel(Customer_Data_XLS_Filename, sheet_name='cust_pkld', header=None)

    #Intialize the Customer_Data struct array, which will store all the customer data.
    #At present, it only contains the bus name, primary bus name, and PKLD value for each customer
    num_customers = len(df)
    Customer_Data_Array = {}
    Customer_Data_Array["Bus_Name_Fully_Qualified"] = np.array([x.lower() for x in df[0].values])
    Customer_Data_Array["Type"] = df3[0].values
    Customer_Data_Array["Phase"] = df4[0].values
    Customer_Data_Array["Bus_Name_Short"] = ['' for _ in range(num_customers)]
    Customer_Data_Array["Primary_Bus_Name_Short"] = ['' for _ in range(num_customers)]
    Customer_Data_Array["Peak_Load"] = Customer_PeakLoad[0].values
    Customer_Data_Array["Bus_Index"] = np.nan * np.ones(num_customers)
    Customer_Data_Array["Primary_Bus_Index"] = np.nan * np.ones(num_customers)
    Customer_Data_Array["x"] = np.nan * np.ones(num_customers)
    Customer_Data_Array["y"] = np.nan * np.ones(num_customers)
    Customer_Data_Array["Zsc1"] = np.nan * np.ones(num_customers, dtype=np.complex)
    Customer_Data_Array["Distance"] = np.nan * np.ones(num_customers)
    Customer_Data_Array["Connected_to_LN_Transformer"] = np.nan * np.ones(num_customers)
    Customer_Data_Array["Primary_Bus_Element_Names"] =      ['' for _ in range(num_customers)]
    Customer_Data_Array["Primary_Bus_Element_LineCode"] =   [0 for _ in range(num_customers)]
    Customer_Data_Array["Primary_Bus_Element_Ampacities"] = [0 for _ in range(num_customers)]
    Customer_Data_Array["Primary_Bus_Element_Ampacities"] = [0 for _ in range(num_customers)]
    Customer_Data_Array["Primary_Bus_Max_Line_Ampacity"]  = [0 for _ in range(num_customers)]
    Customer_Data_Array["Primary_Bus_Max_Line_Name"]      = ['' for _ in range(num_customers)]
    Customer_Data_Array["Primary_Bus_Max_Line_LineCode"]  = ['' for _ in range(num_customers)]

    #Create a graph (adjacency matrix form) of the DSS circuit with edge weights corresponding to electrical distances
    DSSCircuit_GraphMatrices = GetGraphMatricesfromOpenDSSCircuit(True)

    for customer_index in range(num_customers):

        #Get the short bus name of the bus to which the customer is attached.
        #This is the part of the fully qualified bus name (which is in the XLS file) before the first period.
        Customer_Data_Array["Bus_Name_Short"][customer_index] = df[0].values[customer_index].split(".")[0].lower()

        if len(df[0].values[customer_index].split(".")) == 2:
            Customer_Data_Array["Connected_to_LN_Transformer"][customer_index] = 1
        else:
            Customer_Data_Array["Connected_to_LN_Transformer"][customer_index] = 0

        #Find the customer bus index, which is the index in the DSS Circuit
        #object whose name matches the short name of the customer bus.
        customer_bus_index_match = np.argwhere(
                                        np.array(
                                            dss.Circuit.AllBusNames()
                                        ) ==
                                        Customer_Data_Array["Bus_Name_Short"][customer_index]
                                    ).flatten()

        if len(customer_bus_index_match) != 1:

            raise ValueError("Customer {id} at bus {bus} was not successfully matched to a bus index, customer_bus_index_match = {match}.".format(
                                   id=customer_index,
                                   bus=Customer_Data_Array["Bus_Name_Short"][customer_index],
                                   match=customer_bus_index_match)
            )

        Customer_Data_Array["Bus_Index"][customer_index] = customer_bus_index_match[0]

        #The customer primary bus name is the text that appears before the first
        #period in each row of the customer bus list (also remove an optional
        #'?' at the end)
        Customer_Data_Array["Primary_Bus_Name_Short"][customer_index] = df2[0].values[customer_index].replace("?","").lower()

        #Find the customer primary bus index, which is the index in the DSS
        #Circuit object whose bus name matches the short name of the customer primary bus.
        customer_primary_bus_index_match = np.argwhere(
                                               np.array(
                                                dss.Circuit.AllBusNames()
                                                ) ==
                                                Customer_Data_Array["Primary_Bus_Name_Short"][customer_index]
                                            ).flatten()

        Customer_Data_Array["Primary_Bus_Index"][customer_index] = customer_primary_bus_index_match[0]

        #Read the x, y, and distance values of the primary bus from the DSS
        #Circuit object and store in the customer data array
        dss.Circuit.SetActiveBus(Customer_Data_Array["Primary_Bus_Name_Short"][customer_index])

        Customer_Data_Array["x"][customer_index] = dss.Bus.X()
        Customer_Data_Array["y"][customer_index] = dss.Bus.Y()
        Customer_Data_Array["Distance"][customer_index] = dss.Bus.Distance()

        #Calculate the magnitude (2-norm) of the customer's feeder impedence
        Customer_Data_Array["Zsc1"][customer_index] = complex(*dss.Bus.Zsc1())
        dss.Bus.ZscRefresh()

    #Create a list of the x and y coordinates of capacitors
    #Select the first capacitor in the DSSCircuit
    if verbose:
        print("Read_DPV_Customer_Data: Finding capacitor buses...")

    #Determine which customers whose primary bus line ampacity is greater than the threshold
    num_customers = len(Customer_Data_Array["Bus_Name_Short"])

    if verbose:
        print("Read_DPV_Customer_Data: Finding primary bus line ampacity of each of {} customers.".format(
                         num_customers))

    for customer_index in range(num_customers):
        if verbose:
            if customer_index%Progess_Update_Every_X_Customers == 0:
                print("Read_DPV_Customer_Data: Processed {id} of {tot} customers...".format(
                                id=customer_index, tot=num_customers))

        #Extract the names and types of the elements incident to this customer's primary bus
        Primary_Bus_Element_Indices = np.argwhere(
                                        np.abs(
                                           DSSCircuit_GraphMatrices["Bus_to_Line_Incidence"][:,
                                                  int(
                                                    Customer_Data_Array["Primary_Bus_Index"][customer_index]
                                                    )
                                                  ]
                                            )
                                        ).flatten()
        Customer_Data_Array["Primary_Bus_Element_Names"][customer_index] = (
                        np.array(
                            DSSCircuit_GraphMatrices["Element_Names"]
                            )[Primary_Bus_Element_Indices]
                       )

        #TO CHECK...
        Primary_Bus_Element_Tokens = [x.split(".")[0]
                            for x in Customer_Data_Array["Primary_Bus_Element_Names"][customer_index]
                            ]

        Num_Primary_Bus_Elements = len(Primary_Bus_Element_Indices)

        #For each element incident to this customer's primary bus, if it is a
        #line, then extract its ampacity. Otherwise, set its ampacity to zero.
        Customer_Data_Array["Primary_Bus_Element_LineCode"][customer_index]   = ['' for _ in range(Num_Primary_Bus_Elements)]
        Customer_Data_Array["Primary_Bus_Element_Ampacities"][customer_index] = np.nan * np.ones(Num_Primary_Bus_Elements)

        for element_index in range(Num_Primary_Bus_Elements):

            if Primary_Bus_Element_Tokens[element_index] == "Line":

                dss.Circuit.SetActiveElement(
                     Customer_Data_Array["Primary_Bus_Element_Names"][customer_index][element_index]
                     )
                Customer_Data_Array["Primary_Bus_Element_LineCode"][customer_index][element_index] = (
                    dss.Lines.LineCode()
                    )

                Customer_Data_Array["Primary_Bus_Element_Ampacities"][customer_index][element_index] = (
                    dss.Lines.NormAmps()
                    )

            else:
                Customer_Data_Array["Primary_Bus_Element_LineCode"][customer_index][element_index] = ''
                Customer_Data_Array["Primary_Bus_Element_Ampacities"][customer_index][element_index] = 0

        #Find the customer's primary bus line with maximum ampacity and record its name and ampacity
        #TO CHANGE....
        Customer_Data_Array["Primary_Bus_Max_Line_Ampacity"][customer_index] = np.max(
                         Customer_Data_Array["Primary_Bus_Element_Ampacities"][customer_index]
                         )

        max_line_index = np.argmax(
                         Customer_Data_Array["Primary_Bus_Element_Ampacities"][customer_index]
                         )

        Customer_Data_Array["Primary_Bus_Max_Line_Name"][customer_index] = (
                 Customer_Data_Array["Primary_Bus_Element_Names"][customer_index][max_line_index]
                 )

        Customer_Data_Array["Primary_Bus_Max_Line_LineCode"][customer_index] = (
                 Customer_Data_Array["Primary_Bus_Element_LineCode"][customer_index][max_line_index]
                 )

    if verbose:
        print("Read_DPV_Customer_Data: Successfully read {ncust} customers from {file}.".format(
                        ncust=num_customers, file=Customer_Data_XLS_Filename)
        )

    return Customer_Data_Array 


