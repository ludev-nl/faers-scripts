import os
import sys

# Add the faers-scripts directory to the Python path
sys.path.append(os.path.join(os.getcwd(), 'faers-scripts'))

# Import the process_data script
import process_data

# Call the main function in the process_data script
process_data.main()
