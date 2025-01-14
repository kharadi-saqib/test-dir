import os
import subprocess
import time

def filter_pair_files(input_file, pair_files):
    """
    Filter and return only the pair files that are present for the given input file.
    
    Parameters:
        input_file (str): The main input file path.
        pair_files (dict): A dictionary specifying possible extensions for each file type.

    Returns:
        list: List of existing pair files.
    """
    file_extension = os.path.splitext(input_file)[1]
    required_extensions = pair_files.get(file_extension, [])

    existing_files = []
    for ext in required_extensions:
        pair_file = input_file.replace(file_extension, ext)
        if os.path.exists(pair_file):
            existing_files.append(pair_file)
        else:
            print(f"Optional pair file not found and will be ignored: {pair_file}")

    return existing_files

def generate_cog(input_file, output_file):
    output_format = "COG"
    output_format_options = [
        "PREDICTOR=2",
        "BIGTIFF=YES",
        "NUM_THREADS=ALL_CPUS",
        "BLOCKSIZE=128",
#        "TILED=YES"
    ]

    try:
        # Construct the gdal_translate command
        command = [
            "gdal_translate",
            "-of", output_format,
        ]
        for option in output_format_options:
            command.extend(["-co", option])
        command.extend([input_file, output_file])

        # Record start time
        start_time = time.time()

        # Run the command and wait for it to complete
        subprocess.run(command, check=True)

        # Record end time
        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"COG generation completed: {output_file}")
        print(f"Time taken: {elapsed_time:.2f} seconds")
    except subprocess.CalledProcessError as e:
        print(f"Error during COG generation: {e}")

# Main logic
pair_files = {
    ".img": [".ige", ".tif.aux.xml", ".rrd", ".rde"],
    ".tif": [".tfw"]
}

input_file = "/home/saqib/CodeRize/FGIC/Tif Files/T39RZH.jp2"
output_file = "/home/saqib/CodeRize/FGIC/test-cog/Output COG/T39RZH_cog.tif"

# Filter and log available pair files
existing_pair_files = filter_pair_files(input_file, pair_files)
print(f"Available pair files: {existing_pair_files}")

# Proceed with COG generation
generate_cog(input_file, output_file)
