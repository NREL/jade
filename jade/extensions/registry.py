
# REGISTER YOUR EXTENSION HERE
EXTENSION_REGISTRY = {
    "demo": {
        "job_execution_class": "demo.autoregression_execution.AutoRegressionExecution",
        "job_configuration_class": "demo.autoregression_configuration.AutoRegressionConfiguration",
        "description": "Country based GDP auto-regression analysis",
    },
    "pydss_simulation": {
        "job_execution_class": "pydss_simulation.pydss_simulation.PyDssSimulation",
        "job_configuration_class": "pydss_simulation.pydss_configuration.PyDssConfiguration",
        "description": "Runs a PyDSS simulation on a DISCO auto-generated configuration.",
    },
    "user_scenario_pydss_simulation": {
        "job_execution_class": "user_scenario_pydss_simulation.user_scenario_pydss_simulation.UserScenarioPyDssSimulation",
        "job_configuration_class": "user_scenario_pydss_configuration.user_scenario_pydss_configuration.UserScenarioPyDssConfiguration",
        "description": "Runs a PyDSS simulation on a user-defined OpenDSS configuration.",
    },
    "hosting_capacity_analysis": {
        "job_execution_class": "hosting_capacity_analysis.hosting_capacity_analysis.HostingCapacityAnalysis",
        "job_configuration_class": "hosting_capacity_analysis.hosting_capacity_configuration.HostingCapacityConfiguration",
        "description": "Runs hosting capacity analysis on the outputs of a batch of pydss_simulation jobs.",
    },
}


def is_registered(extension):
    """Check if the extension get registered"""
    if extension not in EXTENSION_REGISTRY:
        return False

    return True


def show_extensions():
    """Show the registered extensions."""
    print("JADE Extensions:")
    for extension, data in sorted(EXTENSION_REGISTRY.items()):
        print(f"  {extension}:  {data['description']}")
