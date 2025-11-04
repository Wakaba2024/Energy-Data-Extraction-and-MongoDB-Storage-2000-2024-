# aep_etl/constants.py
"""
Constants: years, country list, etc.
"""

# Years of interest
YEARS = list(range(2000, 2025))  # 2000–2024 inclusive

# List of all African countries (from World Bank / UN)
AFRICAN_COUNTRIES = [
    "Algeria","Angola","Benin","Botswana","Burkina Faso","Burundi","Cabo Verde",
    "Cameroon","Central African Republic","Chad","Comoros","Congo","Congo, Dem. Rep.",
    "Côte d’Ivoire","Djibouti","Egypt","Equatorial Guinea","Eritrea","Eswatini",
    "Ethiopia","Gabon","Gambia","Ghana","Guinea","Guinea-Bissau","Kenya","Lesotho",
    "Liberia","Libya","Madagascar","Malawi","Mali","Mauritania","Mauritius","Morocco",
    "Mozambique","Namibia","Niger","Nigeria","Rwanda","São Tomé and Príncipe","Senegal",
    "Seychelles","Sierra Leone","Somalia","South Africa","South Sudan","Sudan","Tanzania",
    "Togo","Tunisia","Uganda","Zambia","Zimbabwe"
]
