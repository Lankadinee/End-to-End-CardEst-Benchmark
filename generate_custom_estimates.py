def main(): 
    # Define the file name
    file_name = "workloads/power/estimates/custom_estimates_exp.txt"

    # Open file in write mode
    numbers = []
    with open(file_name, "w") as file:
        for num in range(0, 2500, 1):  # Start at 1000, go up to 1,000,000, step by 1000
            num = int(2**(num/100))
            if num not in numbers:
                numbers.append(num)
                file.write(f"{num}\n")


    print(f"File '{file_name}' successfully created!")

def main_2():
    file_name = "workloads/custom/estimates/custom_estimates_1_1000.txt"

    with open(file_name, "w") as file:
        for num in range(1, 1001):
            file.write(f"{num}\n")

if __name__ == "__main__":
    main()
    # main_2()
    print("Done!")
