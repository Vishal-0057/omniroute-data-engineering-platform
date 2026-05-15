import boto3

bucket="omniroute-data-lake-6600"

files={

"generate_datasets.py":"raw/genrate_data/generate_datasets.py",
"vehicle_registry.csv":"raw/vehicle_registry/vehicle_registry.csv",

"vehicle_assignment.csv":"raw/vehicle_assignment/vehicle_assignment.csv",

"maintenance_logs.csv":"raw/maintenance_logs/maintenance_logs.csv",

"fuel_transactions.csv":"raw/fuel_transactions/fuel_transactions.csv",

"restricted_zones.json":"raw/restricted_zones/restricted_zones.json"
}

s3=boto3.client("s3")

for local_file,s3_path in files.items():

    s3.upload_file(local_file,bucket,s3_path)

    print(f"uploaded {local_file}")

print("all files uploaded")
