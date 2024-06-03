import csv
import time
from datetime import datetime
import boto3
import random
import folium
import webbrowser

# List of latitude and longitude pairs for new locations
coordinates = [
    (43.3959654929088, 3.716288415074448),
    (43.50164684958629, 3.868723710247501),
    (43.509615306980386, 4.104929753218357),
    (43.32407663115837, 4.846506864871045),
    (43.41553590723597, 4.93587126789594),
    (43.32692245765735, 5.030277128833238),
    (43.323588599894386, 5.196174806791015),
    (43.35413624942009, 5.314865998871046),
    (43.28038344633476, 5.326706185082117),
    (43.26147761698216, 5.289124818791339),
    (40.00818870046963, 4.087364032703926),
    (39.96324889865491, 3.2148390950790553),
    (39.12137303453927, 1.5430616211387964),
    (38.84323442710925, 0.11961060621745259),
    (38.995466658839334, -0.14856863615037347),
    (39.45764767615607, -0.3220012143611796),
    (39.6501433867352, -0.20966804552766402),
    (40.20880362527458, 0.26216155711675),
    (41.36949074231026, 2.1822533990046584),
    (43.27486197510666, 3.500666500087192)
]

# Function to generate climate and vessel data
def generate_data(previous_minute, previous_temperature, coordinates):
    current_minute = previous_minute + 1
    temperature = previous_temperature + 1.0  # Increase temperature by 1 degree Celsius every minute
    coord_index = current_minute % len(coordinates)
    latitude, longitude = coordinates[coord_index]

    # Generate random speed between 0.0 and 60.0 knots
    speed = random.uniform(0.0, 60.0)

    # Randomly update course over ground, heading, and other data
    course_over_ground = random.uniform(0.0, 360.0)
    distance_to_cpa = random.uniform(0.0, 100.0)
    vessel_names = ["New Waves", "Ocean Star", "Sea Serpent"]  # Random vessel names
    vessel_name = random.choice(vessel_names)
    heading = random.uniform(0.0, 360.0)
    countries = ["France", "France", "France", "France", "France", "France", "France", "France", "France", "France", "Spain", "Spain", "Spain", "Spain", "Spain", "Spain", "Spain", "Spain", "Spain", "Spain"]
    country = countries[coord_index]

    # Add a timestamp for debugging purposes
    timestamp = str(datetime.now())

    # Generate random new metric value
    new_metric_value = random.uniform(0, 100)

    # Update new timestamp
    new_timestamp = timestamp

    return {
        "Temperature (°C)": temperature,
        "Longitude": longitude,
        "Latitude": latitude,
        "Speed (knots)": speed,
        "Vessel Name": vessel_name,
        "Course Over Ground (degrees)": course_over_ground,
        "Vessel Dimensions": "100m x 20m",  # You can update this to be random if needed
        "Navigational Status": "Underway",  # You can update this to be random if needed
        "Heading (degrees)": heading,
        "Distance to CPA (nautical miles)": distance_to_cpa,
        "Country": country,
        "Minute": current_minute,
        "Timestamp": timestamp,
        "new_metric_value": new_metric_value,
        "new_timestamp": new_timestamp
    }

# Function to upload data to S3 bucket
def upload_to_s3(data, bucket_name, object_name, s3):
    try:
        # Read existing CSV file from S3
        existing_data = ""
        try:
            existing_object = s3.get_object(Bucket=bucket_name, Key=object_name)
            existing_data = existing_object['Body'].read().decode('utf-8')
        except s3.exceptions.NoSuchKey:
            print(f"No existing file found in bucket {bucket_name} with name {object_name}. Creating new file.")
            pass

        # If no existing data, write headers
        if not existing_data:
            headers = [
                "Temperature (°C)",
                "Longitude",
                "Latitude",
                "Speed (knots)",
                "Vessel Name",
                "Course Over Ground (degrees)",
                "Vessel Dimensions",
                "Navigational Status",
                "Heading (degrees)",
                "Distance to CPA (nautical miles)",
                "Country",
                "Minute",
                "Timestamp",
                "new_metric_value",
                "new_timestamp"
            ]
            existing_data = ",".join(headers) + "\n"

        # Format new data as CSV row
        csv_data = [
            data['Temperature (°C)'],
            data['Longitude'],
            data['Latitude'],
            data['Speed (knots)'],
            data['Vessel Name'],
            data['Course Over Ground (degrees)'],
            data['Vessel Dimensions'],
            data['Navigational Status'],
            data['Heading (degrees)'],
            data['Distance to CPA (nautical miles)'],
            data['Country'],
            data['Minute'],
            data['Timestamp'],
            data['new_metric_value'],
            data['new_timestamp']
        ]
        csv_row = ",".join(map(str, csv_data)) + "\n"

        # Combine existing data with new data
        updated_data = existing_data + csv_row

        # Upload the updated CSV data to S3
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=updated_data.encode('utf-8'))
        print(f"Data appended to S3 bucket {bucket_name} in file {object_name}.")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")

# Function to update the CSV file with temperature and minute data
def update_data_csv(bucket_name, object_name="daily_temperature.csv"):
    s3 = boto3.client('s3')
    try:
        previous_minute = 0
        temperature = 10.0  # Starting temperature
        first_run = True
        while True:
            data = generate_data(previous_minute, temperature, coordinates)
            if first_run:
                print(f"Generated data: {data}")  # Print generated data for verification
                first_run = False
            upload_to_s3(data, bucket_name,object_name, s3)
            previous_minute = data['Minute']
            temperature = data['Temperature (°C)']  # Update temperature for next iteration
            time.sleep(60)  # Wait for 60 seconds before generating next data
    except KeyboardInterrupt:
        print("Data generation stopped by user.")
    except Exception as e:
        print(f"Error generating data: {e}")

if __name__ == "__main__":
    bucket_name = "satya010101"  # Replace 'satya010101' with your actual bucket name

    print("Data generation started.")
    # Call the function to start updating the CSV file and upload to S3
    update_data_csv(bucket_name)
    print("Data generation stopped.")

    # List of latitude, longitude, and country tuples for new locations
    locations = [
        (43.3959654929088, 3.716288415074448, "France"),
        (43.50164684958629, 3.868723710247501, "France"),
        (43.509615306980386, 4.104929753218357, "France"),
        (43.32407663115837, 4.846506864871045, "France"),
        (43.41553590723597, 4.93587126789594, "France"),
        (43.32692245765735, 5.030277128833238, "France"),
        (43.323588599894386, 5.196174806791015, "France"),
        (43.35413624942009, 5.314865998871046, "France"),
        (43.28038344633476, 5.326706185082117, "France"),
        (43.26147761698216, 5.289124818791339, "France"),
        (40.00818870046963, 4.087364032703926, "Spain"),
        (39.96324889865491, 3.2148390950790553, "Spain"),
        (39.12137303453927, 1.5430616211387964, "Spain"),
        (38.84323442710925, 0.11961060621745259, "Spain"),
        (38.995466658839334, -0.14856863615037347, "Spain"),
        (39.45764767615607, -0.3220012143611796, "Spain"),
        (39.6501433867352, -0.20966804552766402, "Spain"),
        (40.20880362527458, 0.26216155711675, "Spain"),
        (41.36949074231026, 2.1822533990046584, "Spain"),
        (43.27486197510666, 3.500666500087192, "France")
    ]

    # Create a map centered at the first coordinates
    mymap = folium.Map(location=[locations[0][0], locations[0][1]], zoom_start=5)

    # Add markers for all locations
    for loc in locations:
        folium.Marker(location=[loc[0], loc[1]], tooltip=loc[2]).add_to(mymap)

    # Save the map to an HTML file
    mymap.save('map.html')

    # Open the HTML file in a web browser
    webbrowser.open('map.html')

    # Wait for 60 seconds before closing
    time.sleep(60)

