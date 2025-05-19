import numpy as np
import matplotlib.pyplot as plt
import sys
import csv

def main():
    # Check if a filename was provided as a command line argument
    if len(sys.argv) < 2:
        print("Usage: python script.py <csv_file>")
        sys.exit(1)
    
    # Get the filename from command line arguments
    filename = sys.argv[1]
    
    # Initialize lists to store data
    x_coords = []
    y_coords = []
    classes = []
    
    # Read the CSV file
    try:
        with open(filename, 'r') as csvfile:
            # Using csv reader with no header
            csv_reader = csv.reader(csvfile)
            for row in csv_reader:
                if len(row) >= 3:  # Ensure row has enough columns
                    x_coords.append(float(row[0]))
                    y_coords.append(float(row[1]))
                    classes.append(row[2])
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)
    
    # Separate points by class
    class_a_x = [x for x, cls in zip(x_coords, classes) if cls == 'A']
    class_a_y = [y for y, cls in zip(y_coords, classes) if cls == 'A']
    class_b_x = [x for x, cls in zip(x_coords, classes) if cls == 'B']
    class_b_y = [y for y, cls in zip(y_coords, classes) if cls == 'B']
    
    # Create a scatter plot
    plt.figure(figsize=(10, 8))
    plt.scatter(class_a_x, class_a_y, color='blue', marker='o', s=100, label='Class A')
    plt.scatter(class_b_x, class_b_y, color='red', marker='x', s=100, label='Class B')
    
    # Add labels and title
    plt.title(f'{filename}')
    plt.grid(True)
    plt.legend()
    
    # Show the plot
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
