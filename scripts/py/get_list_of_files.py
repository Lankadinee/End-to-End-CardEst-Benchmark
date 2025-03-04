import subprocess


def get_file_list(container_name, directory):
    # Run docker exec command
    result = subprocess.run(
        ["docker", "exec", container_name, "ls", "-1", directory],
        capture_output=True,
        text=True
    )

    # Print or process the output
    files = []
    if result.returncode == 0:
        files = result.stdout.strip().split("\n")
        # print("Files inside the container:", files)
    else:
        print("Error:", result.stderr)
        raise Exception("Error while listing files inside the container")

    return files

def get_all_unprocessed_txt_files(container_name, directory):
    files = get_file_list(container_name, directory)
    all_txt_files = [file.split('.')[0] for file in files if file.endswith(".txt")]
    all_excel_files = [file.split('.')[0] for file in files if file.endswith(".xlsx")]
    unprocessed_files = list(set(all_txt_files) - set(all_excel_files))
    return [f"{fname}.txt" for fname in unprocessed_files]

if __name__ == "__main__":
    unproc_files = get_all_unprocessed_txt_files("ce-benchmark-ceb1-forest", "/var/lib/pgsql/13.1/data/")
    print("Unprocessed files:", unproc_files)