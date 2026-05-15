import pandas as pd
import random
from datetime import datetime, timedelta
import json

############################
# CONFIG
############################

NUM_VEHICLES = 50
NUM_DRIVERS = 30
START_DATE = datetime(2026,1,1)
END_DATE = datetime(2026, 3, 31)

models = [
"Volvo VNL",
"Freightliner M2",
"Isuzu N-Series",
"Tata Prima",
"Ashok Leyland"
]

fuel_types=["Diesel","LNG","CNG"]

regions=["North","South","East","West"]

############################
# 1 vehicle_registry
############################

vehicles=[]

for i in range(NUM_VEHICLES):

    vin=f"VIN{i:04}"

    vehicles.append({
        "vin":vin,
        "model":random.choice(models),
        "mfg_year":random.randint(2018,2024),
        "fuel_type":random.choice(fuel_types)
    })

vehicle_df=pd.DataFrame(vehicles)

vehicle_df.to_csv("vehicle_registry.csv",index=False)

############################
# 2 vehicle_assignment
############################

assignments=[]

for v in vehicles:

    vin=v["vin"]

    current_date=START_DATE

    for _ in range(random.randint(1,3)):

        duration=random.randint(30,120)

        start=current_date
        end=start+timedelta(days=duration)

        assignments.append({
            "vin":vin,
            "driver_id":f"DRV{random.randint(1,NUM_DRIVERS):03}",
            "start_timestamp":int(start.timestamp()),
            "end_timestamp":int(end.timestamp()),
            "daily_rate":random.randint(300,700),
            "region":random.choice(regions)
        })

        current_date=end

assignment_df=pd.DataFrame(assignments)

assignment_df.to_csv("vehicle_assignment.csv",index=False)

############################
# 3 maintenance_logs
############################
service_types = [
    "Engine",
    "Oil Change",
    "Brake Service",
    "Tire Replacement",
    "General Inspection"
]
maintenance_data = []

for v in vehicles:

    # each vehicle gets 1–3 maintenance events
    num_services = random.randint(1, 3)
    vin = v["vin"] 
    for _ in range(num_services):

        random_days = random.randint(
            0,
            (END_DATE - START_DATE).days
        )

        service_date = START_DATE + timedelta(days=random_days)

        maintenance_data.append({
            "vin": vin,
            "service_date": service_date.strftime("%Y-%m-%d"),
            "service_type": random.choice(service_types)
        })

#########################################
# CREATE DATAFRAME
#########################################

df = pd.DataFrame(maintenance_data)

# remove duplicates (same vin + date)
df = df.drop_duplicates(subset=["vin", "service_date"])

#########################################
# SAVE FILE
#########################################

df.to_csv("maintenance_logs.csv", index=False)

print("New maintenance_logs.csv generated successfully")
print(f"Total rows: {len(df)}")
############################
# 4 fuel_transactions
############################

fuel_data=[]

for v in vehicles:

    odometer=10000

    for day in range(30):

        fuel=random.uniform(40,120)

        distance=random.uniform(200,600)

        odometer+=distance

        fuel_data.append({
            "transaction_id":f"TXN{random.randint(1000,9999)}",
            "vin":v["vin"],
            "fuel_liters":round(fuel,2),
            "odometer_reading":round(odometer,2),
            "timestamp":(START_DATE+timedelta(days=day)).strftime("%Y-%m-%d %H:%M:%S")
        })

fuel_df=pd.DataFrame(fuel_data)

fuel_df.to_csv("fuel_transactions.csv",index=False)

############################
# 5 restricted zones
############################

zone_coordinates = {
    "North": {
        "min_lat": 28.65,
        "max_lat": 28.85,
        "min_long": 77.10,
        "max_long": 77.30
    },
    "South": {
        "min_lat": 28.45,
        "max_lat": 28.65,
        "min_long": 77.10,
        "max_long": 77.30
    },
    "East": {
        "min_lat": 28.55,
        "max_lat": 28.75,
        "min_long": 77.30,
        "max_long": 77.50
    },
    "West": {
        "min_lat": 28.55,
        "max_lat": 28.75,
        "min_long": 76.90,
        "max_long": 77.10
    }
}

zones = []

for region in regions:
    zones.append({
        "zone_name": region,  # IMPORTANT: matches vehicle_assignment
        "min_lat": zone_coordinates[region]["min_lat"],
        "max_lat": zone_coordinates[region]["max_lat"],
        "min_long": zone_coordinates[region]["min_long"],
        "max_long": zone_coordinates[region]["max_long"]
    })

with open("restricted_zones.json", "w") as f:
    json.dump(zones, f, indent=2)

#########################################
# DONE
#########################################

print("All datasets generated successfully")
