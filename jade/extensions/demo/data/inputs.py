"""
Script for creating inputs.json file
"""
import json


COUNTRIES = [
    "Australia",
    "Brazil",
    "Canada",
    "China",
    "France",
    "Germany",
    "India",
    "Italy",
    "Japan",
    "Russia",
    "United Kingdom",
    "United States",
]

INPUTS_FILE = "inputs.json"


def main():
    """Create inputs.json file"""
    inputs = []
    for country in COUNTRIES:
        filename = country.replace(" ", "_").lower() + ".csv"
        parameter = {"country": country, "data": f"gdp/countries/{filename}"}

        # Generate wrong path for raising error while running job
        if country in ["Australia", "Canada"]:
            parameter["data"] = "x" + parameter["data"]

        inputs.append(parameter)

    with open(INPUTS_FILE, "w") as f:
        json.dump(inputs, f, indent=2)

    print("inputs.json created.")


if __name__ == "__main__":
    main()
